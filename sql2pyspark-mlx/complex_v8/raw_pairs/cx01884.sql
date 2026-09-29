WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.ms_played) AS prior_ms_played_7d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_id,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        prior_rows_7d,
        ROUND(STDDEV(prior_rows_7d), 2) AS stddev_prior_rows_7d,
        MIN(played_at) AS first_played_at,
        MIN(prior_ms_played_7d) AS min_prior_ms_played_7d,
        COUNT(*) AS cnt
    FROM in_window
    GROUP BY listener_id, DATE_TRUNC('MONTH', played_at), prior_rows_7d
)
SELECT
    listener_id,
    played_at_month,
    prior_rows_7d,
    stddev_prior_rows_7d,
    first_played_at,
    min_prior_ms_played_7d,
    cnt,
    DATEDIFF(DATE '2025-06-30', first_played_at) AS days_since_first_played_at,
    stddev_prior_rows_7d + cnt AS stddev_prior_rows_7d_plus_cnt
FROM stats
