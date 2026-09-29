WITH filtered_tracks AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE release_date IS NOT NULL
      AND duration_sec < 353
),
enriched AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        a.genre AS artist_genre
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        duration_sec,
        moods,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_genre, 2) AS duration_sec_vs_artist_genre_avg,
        COUNT(*) OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER w_artist_genre_by_duration_sec, 3) AS duration_sec_cume_dist,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS duration_sec_share_of_artist_id_is_explicit,
        MIN(duration_sec) OVER (PARTITION BY artist_genre ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        COUNT(*) OVER w_artist_id_is_explicit AS rows_in_artist_id_is_explicit,
        ROUND(AVG(duration_sec) OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_3_prior,
        MIN(duration_sec) OVER (PARTITION BY artist_genre) AS artist_genre_min_duration_sec,
        ROUND(AVG(duration_sec) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_6_prior
    FROM enriched
    WINDOW w_artist_genre AS (PARTITION BY artist_genre), w_artist_genre_by_duration_sec AS (PARTITION BY artist_genre ORDER BY duration_sec ASC, id), w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        first_known_duration_sec - duration_sec_cume_dist AS first_known_duration_sec_minus_duration_sec_cume,
        FLOOR(running_count / 5) * 5 AS running_count_bucket
    FROM metrics
),
summary AS (
    -- Summarise per group
    SELECT
        CASE WHEN min_duration_sec_so_far >= 3584 THEN 'HIGH' WHEN min_duration_sec_so_far >= 912 THEN 'MEDIUM' ELSE 'LOW' END AS min_duration_sec_so_far_tier,
        COUNT(DISTINCT id) AS distinct_ids
    FROM tagged
    GROUP BY 1
)
SELECT
    min_duration_sec_so_far_tier,
    distinct_ids,
    ABS(distinct_ids - 6) AS distinct_ids_distance
FROM summary
