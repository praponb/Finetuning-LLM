WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.ms_played) AS prior_ms_played_1d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_id,
        TO_DATE(played_at) AS played_at_day,
        prior_rows_1d,
        MIN(played_at) AS first_played_at,
        COUNT(*) AS cnt,
        MAX(played_at) AS last_played_at,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids
    FROM covered
    WHERE prior_ms_played_1d < 1700000
    GROUP BY listener_id, TO_DATE(played_at), prior_rows_1d
)
-- Final projection
SELECT
    listener_id,
    prior_rows_1d,
    first_played_at,
    distinct_listener_ids
FROM rollup_base
