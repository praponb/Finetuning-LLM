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
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_id,
        prior_rows_12h,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        MIN(prior_rows_12h) AS min_prior_rows_12h,
        ROUND(STDDEV(prior_rows_12h), 2) AS stddev_prior_rows_12h,
        MIN(played_at) AS first_played_at
    FROM covered
    GROUP BY listener_id, prior_rows_12h, DATE_TRUNC('MONTH', played_at)
    HAVING MAX(prior_rows_12h) > 2
),
scored AS (
    -- Label and score rows
    SELECT
        listener_id,
        prior_rows_12h,
        played_at_month,
        min_prior_rows_12h,
        first_played_at,
        DATE_FORMAT(first_played_at, 'yyyy-MM-dd HH:00') AS first_played_at_label
    FROM grouped
)
SELECT
    listener_id,
    prior_rows_12h,
    played_at_month,
    min_prior_rows_12h,
    first_played_at,
    first_played_at_label,
    TO_DATE(played_at_month) AS played_at_month_date
FROM scored
ORDER BY played_at_month, listener_id, prior_rows_12h
LIMIT 5
