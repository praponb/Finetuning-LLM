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
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        listener_id,
        prior_rows_12h,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        PERCENTILE_APPROX(prior_rows_12h, 0.5) AS median_prior_rows_12h,
        ROUND(AVG(prior_rows_12h), 2) AS avg_prior_rows_12h
    FROM covered
    GROUP BY 1, 2, 3
    HAVING AVG(prior_ms_played_12h) > 200000
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER w_prior_rows_12h_listener_id_b AS avg_prior_rows_12h_decile,
        NTH_VALUE(distinct_listener_ids, 2) OVER (ORDER BY median_prior_rows_12h, played_at_month, listener_id, prior_rows_12h ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_listener_ids,
        LAG(median_prior_rows_12h) OVER w_all_by_median_prior_rows_12h_2 AS prev_median_prior_rows_12h
    FROM grouped
    WINDOW w_prior_rows_12h_listener_id_b AS (PARTITION BY prior_rows_12h, listener_id ORDER BY avg_prior_rows_12h NULLS LAST, played_at_month, listener_id, prior_rows_12h), w_all_by_median_prior_rows_12h_2 AS (ORDER BY median_prior_rows_12h, played_at_month, listener_id, prior_rows_12h)
)
-- Final projection
SELECT
    played_at_month,
    prior_rows_12h,
    median_prior_rows_12h,
    second_distinct_listener_ids,
    prev_median_prior_rows_12h,
    COALESCE(median_prior_rows_12h, 0) AS median_prior_rows_12h_filled,
    DATEDIFF(DATE '2025-06-30', played_at_month) AS days_since_played_at_month
FROM with_windows
WHERE TO_DATE(played_at_month) < DATE '2025-02-09'
ORDER BY played_at_month, prior_rows_12h
