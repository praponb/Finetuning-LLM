WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
),
combined_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.is_explicit_label,
        a.country AS artist_country,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM recent_tracks s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER (PARTITION BY artist_genre), 0), 4) AS artist_debut_year_share_of_artist_genre,
        NTILE(5) OVER w_artist_genre_by_bpm AS bpm_quintile,
        LAG(artist_debut_year) OVER (ORDER BY bpm, id) AS prev_artist_debut_year,
        FIRST_VALUE(artist_debut_year) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_debut_year,
        LAG(artist_debut_year) OVER (PARTITION BY artist_genre ORDER BY bpm ASC, id) AS prev_artist_debut_year_2,
        ROUND(CUME_DIST() OVER (ORDER BY bpm, id), 3) AS bpm_cume_dist,
        ROUND(AVG(artist_debut_year) OVER (ORDER BY bpm, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS artist_debut_year_avg_2_prior,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_artist_genre, 0), 4) AS bpm_share_of_artist_genre
    FROM combined_details
    WINDOW w_artist_genre_by_bpm AS (PARTITION BY artist_genre ORDER BY bpm ASC, id), w_artist_genre AS (PARTITION BY artist_genre)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(artist_genre, '#', CAST(id AS STRING)) AS artist_genre_ref,
        COALESCE(artist_debut_year, 0) AS artist_debut_year_filled,
        COALESCE(artist_country, 'UNKNOWN') AS artist_country_or_unknown
    FROM windowed
    WHERE bpm_quintile < 8
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(first_artist_debut_year, 2) OVER (ORDER BY bpm_share_of_artist_genre DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_artist_debut_year,
        COUNT(*) OVER (PARTITION BY artist_genre) AS rows_in_artist_genre,
        SUM(first_artist_debut_year) OVER (ORDER BY bpm_share_of_artist_genre DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS first_artist_debut_year_sum_4,
        ROUND(AVG(first_artist_debut_year) OVER (PARTITION BY artist_genre), 2) AS artist_genre_avg_first_artist_debut_year,
        DENSE_RANK() OVER (ORDER BY bpm_share_of_artist_genre DESC, id) AS bpm_share_of_artist_genre_dense_rank
    FROM scored
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        bpm_quintile,
        bpm_share_of_artist_genre_dense_rank,
        COUNT(*) FILTER (WHERE LOWER(title) LIKE '%t %') AS n_lower_title_like_t,
        ARRAY_SORT(COLLECT_SET(artist_genre)) AS artist_genre_list
    FROM with_trends
    GROUP BY 1, 2, 3
    HAVING SUM(artist_debut_year_filled) > 2002
)
SELECT
    artist_id,
    bpm_quintile,
    bpm_share_of_artist_genre_dense_rank,
    n_lower_title_like_t,
    artist_genre_list,
    ARRAY_JOIN(ARRAY_SORT(artist_genre_list), ',') AS artist_genre_list_text
FROM rollup_base
ORDER BY bpm_share_of_artist_genre_dense_rank DESC
