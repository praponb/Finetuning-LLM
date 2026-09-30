WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
),
with_parent AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.duration_sec_filled,
        a.country AS artist_country,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM prepared s
    LEFT JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_duration_sec_ties, 3) AS duration_sec_percent_rank,
        ROW_NUMBER() OVER w_all_by_duration_sec AS rn,
        SUM(duration_sec) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        LAST_VALUE(artist_debut_year) IGNORE NULLS OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_artist_debut_year,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        COUNT(*) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(duration_sec) OVER (ORDER BY duration_sec ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_7,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec DESC, id), 3) AS duration_sec_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec ASC NULLS LAST, id), 3) AS duration_sec_cume_dist_2
    FROM with_parent
    WINDOW w_all_by_duration_sec_ties AS (ORDER BY duration_sec ASC NULLS LAST), w_all_by_duration_sec AS (ORDER BY duration_sec, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_country,
        artist_id,
        SUM(CASE WHEN LENGTH(TRIM(title)) >= 6 THEN 1 ELSE 0 END) AS n_length_trim_title_6,
        COUNT(DISTINCT artist_genre) AS distinct_artist_genres,
        ARRAY_SORT(COLLECT_SET(artist_country)) AS artist_country_list,
        ROUND(AVG(duration_sec_percent_rank), 2) AS avg_duration_sec_percent_rank,
        PERCENTILE_APPROX(rn, 0.5) AS median_rn
    FROM metrics
    GROUP BY artist_country, artist_id
)
SELECT
    artist_country,
    artist_id,
    n_length_trim_title_6,
    distinct_artist_genres,
    avg_duration_sec_percent_rank
FROM per_group
ORDER BY avg_duration_sec_percent_rank DESC, n_length_trim_title_6, artist_country, artist_id
LIMIT 5
