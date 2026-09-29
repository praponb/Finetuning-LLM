WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'chill')
      AND artist_id <= 14
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE NOT a.is_verified
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY artist_country) AS rows_in_artist_country,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY artist_debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        ROUND(CUME_DIST() OVER w_artist_id_by_bpm, 3) AS bpm_cume_dist,
        LAG(duration_sec, 1, 0) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS prev_duration_sec,
        MAX(duration_sec) OVER (ORDER BY bpm, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_max_3,
        MAX(artist_debut_year) OVER (PARTITION BY artist_country) AS artist_country_max_artist_debut_year,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY artist_country)) AS distinct_artist_id_in_artist_country,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER w_artist_country, 0), 4) AS artist_debut_year_share_of_artist_country,
        ROUND(CUME_DIST() OVER (ORDER BY artist_debut_year ASC), 3) AS artist_debut_year_cume_dist,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_country), 2) AS artist_country_avg_bpm,
        bpm - LAG(bpm) OVER (ORDER BY artist_debut_year ASC, id) AS bpm_change,
        NTILE(3) OVER w_artist_is_verified_artist_co AS duration_sec_tercile,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id_artist_country, 2) AS duration_sec_vs_artist_id_artist_country_avg,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_id, artist_country), 2) AS artist_id_artist_country_avg_artist_debut_year,
        RANK() OVER w_all_by_bpm AS bpm_rank
    FROM joined_artist
    WINDOW w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm DESC, id), w_artist_country AS (PARTITION BY artist_country), w_artist_is_verified_artist_co AS (PARTITION BY artist_is_verified, artist_country ORDER BY duration_sec DESC, id), w_artist_id_artist_country AS (PARTITION BY artist_id, artist_country), w_all_by_bpm AS (ORDER BY bpm, id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_is_verified,
        artist_id,
        COUNT(*) AS n_rows,
        SUM(distinct_artist_id_in_artist_country) AS total_distinct_artist_id_in_artist_country,
        COUNT(DISTINCT artist_country) AS distinct_artist_countrys,
        ROUND(SUM(bpm_rank) / NULLIF(SUM(artist_debut_year_share_of_artist_country), 0), 3) AS bpm_rank_to_artist_debut_year_share_of_artist_co,
        SUM(CASE WHEN artist_is_verified = FALSE THEN artist_debut_year ELSE 0 END) AS artist_debut_year_artist_is_verified_false,
        MAX(artist_debut_year_share_of_artist_country) AS max_artist_debut_year_share_of_artist_country
    FROM metrics
    WHERE artist_id <= 8
    GROUP BY artist_is_verified, artist_id
)
SELECT
    artist_is_verified,
    artist_id,
    n_rows,
    total_distinct_artist_id_in_artist_country,
    distinct_artist_countrys,
    bpm_rank_to_artist_debut_year_share_of_artist_co,
    max_artist_debut_year_share_of_artist_country,
    ROUND(distinct_artist_countrys / NULLIF(n_rows, 0), 3) AS distinct_artist_countrys_per_n_rows
FROM totals
ORDER BY artist_id DESC, n_rows DESC
