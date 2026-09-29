WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE source NOT IN ('search', 'playlist')
      AND is_skipped = FALSE
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        played_at,
        is_skipped,
        ms_played_bucket,
        ms_played_bucket - LAG(ms_played_bucket) OVER (PARTITION BY is_skipped, listener_id ORDER BY ms_played_bucket DESC NULLS FIRST, id) AS ms_played_bucket_change,
        DENSE_RANK() OVER (PARTITION BY is_skipped, listener_id ORDER BY ms_played_bucket DESC NULLS FIRST) AS ms_played_bucket_dense_rank,
        RANK() OVER w_is_skipped_listener_id_by_ms AS ms_played_bucket_rank
    FROM scoped_plays
    WINDOW w_is_skipped_listener_id_by_ms AS (PARTITION BY is_skipped, listener_id ORDER BY ms_played_bucket DESC NULLS FIRST, id)
),
with_details_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.is_skipped,
        s.ms_played_bucket_change,
        s.ms_played_bucket_rank,
        l.signup_date AS listener_signup_date
    FROM with_trends s
    JOIN listeners l ON l.id = s.listener_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        is_skipped,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(ms_played_bucket_rank) / NULLIF(SUM(ms_played_bucket_change), 0), 3) AS ms_played_bucket_rank_to_ms_played_bucket_change
    FROM with_details_listener
    GROUP BY is_skipped
    HAVING MAX(ms_played_bucket_rank) > 2
)
-- Final projection
SELECT is_skipped, distinct_ids, ABS(distinct_ids - 4) AS distinct_ids_distance
FROM stats
ORDER BY distinct_ids_distance, is_skipped
LIMIT 20
