WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.ms_played) AS prior_ms_played_12h
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.listener_id, a.played_at
),
tagged AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM joined_ranges
)
SELECT
    listener_id,
    prior_rows_12h,
    GROUPING(listener_id) AS is_total_listener_id,
    ROUND(STDDEV(days_since_played_at), 2) AS stddev_days_since_played_at
FROM tagged
GROUP BY ROLLUP(listener_id, prior_rows_12h)
