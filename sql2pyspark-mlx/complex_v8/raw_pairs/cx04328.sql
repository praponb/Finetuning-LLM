WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods
    FROM tracks
    WHERE is_explicit = TRUE
      AND YEAR(release_date) = 2025
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.moods,
        a.genre AS artist_genre,
        a.country AS artist_country
    FROM scoped s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.genre = 'Jazz'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY artist_id, artist_genre) AS artist_id_artist_genre_min_duration_sec,
        ROUND(CUME_DIST() OVER w_artist_id_artist_genre_by_du, 3) AS duration_sec_cume_dist,
        MAX(duration_sec) OVER (PARTITION BY artist_id, artist_genre) AS artist_id_artist_genre_max_duration_sec,
        SIZE(COLLECT_SET(artist_country) OVER w_artist_id_artist_genre) AS distinct_artist_country_in_artist_id_artist_genr,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_country), 0), 4) AS duration_sec_share_of_artist_country,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_country, artist_genre), 0), 4) AS duration_sec_share_of_artist_country_artist_genr,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY artist_country)) AS distinct_artist_id_in_artist_country,
        COUNT(*) OVER (PARTITION BY artist_id, artist_genre) AS rows_in_artist_id_artist_genre,
        RANK() OVER (ORDER BY duration_sec DESC) AS duration_sec_rank,
        LAG(duration_sec, 2) OVER (ORDER BY duration_sec DESC, id) AS prev2_duration_sec,
        NTILE(10) OVER (ORDER BY duration_sec DESC, id) AS duration_sec_decile
    FROM combined_details
    WINDOW w_artist_id_artist_genre_by_du AS (PARTITION BY artist_id, artist_genre ORDER BY duration_sec ASC, id), w_artist_id_artist_genre AS (PARTITION BY artist_id, artist_genre)
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    artist_genre,
    artist_country,
    second_duration_sec,
    artist_id_artist_genre_min_duration_sec,
    last_known_duration_sec,
    distinct_artist_id_in_artist_country,
    duration_sec_rank,
    prev2_duration_sec,
    duration_sec_decile
FROM ranked
ORDER BY artist_id_artist_genre_min_duration_sec, prev2_duration_sec DESC, id
LIMIT 10
