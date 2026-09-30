WITH covered AS (
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
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        played_at,
        prior_rows_12h,
        prior_ms_played_12h,
        played_at + INTERVAL 2 HOURS AS played_at_plus_2h,
        LEAST(prior_ms_played_12h, prior_rows_12h) AS least_prior_ms_played_12h_prior_rows_12h,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM covered
)
SELECT
    id,
    listener_id,
    prior_rows_12h,
    prior_ms_played_12h,
    played_at_plus_2h,
    days_since_played_at,
    ABS(prior_rows_12h - 6) AS prior_rows_12h_distance
FROM enriched
ORDER BY prior_rows_12h DESC, id
