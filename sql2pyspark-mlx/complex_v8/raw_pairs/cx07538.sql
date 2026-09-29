WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        INITCAP(TRIM(title)) AS title_clean
    FROM tracks
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.title_clean,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM base s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        duration_sec,
        is_explicit,
        title_clean,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec), 3) AS duration_sec_cume_dist,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC), 3) AS duration_sec_cume_dist_2,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_country), 0), 4) AS duration_sec_share_of_artist_country,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_genre), 0), 4) AS duration_sec_share_of_artist_genre,
        LAG(duration_sec, 3, 0) OVER w_all_by_duration_sec AS prev3_duration_sec,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id) AS duration_sec_change,
        SUM(duration_sec) OVER (PARTITION BY artist_country) AS artist_country_sum_duration_sec
    FROM enriched_artist
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec, id)
)
-- Final projection
SELECT
    id,
    title,
    is_explicit,
    title_clean,
    second_duration_sec,
    duration_sec_cume_dist_2,
    duration_sec_share_of_artist_genre,
    duration_sec_change,
    artist_country_sum_duration_sec,
    INITCAP(TRIM(title_clean)) AS title_clean_clean,
    LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
FROM with_windows
WHERE duration_sec_share_of_artist_genre BETWEEN 0.3 AND 0.8
ORDER BY duration_sec_cume_dist_2 DESC NULLS FIRST, title_clean_clean
