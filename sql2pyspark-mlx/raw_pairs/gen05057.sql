WITH filtered AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        CAST(paid_at AS DATE) AS paid_at_day,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM payments
    WHERE method IS NOT NULL
),
enriched AS (
    SELECT
        *,
        AVG(amount) OVER w_running AS running_avg_amount,
        MAX(amount) OVER w_running AS running_max_amount,
        MIN(amount) OVER w_running AS running_min_amount
    FROM filtered
    WINDOW w_running AS (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS rn
    FROM enriched
    QUALIFY rn = 1
),
enriched_payments AS (
    SELECT
        id,
        method,
        paid_at,
        running_max_amount,
        AVG(running_avg_amount) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS running_avg_amount_avg_2_prior,
        FIRST_VALUE(running_max_amount, TRUE) OVER (PARTITION BY method ORDER BY paid_at_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_max_amount,
        CUME_DIST() OVER (PARTITION BY method ORDER BY running_max_amount NULLS FIRST) AS running_max_amount_cume_dist
    FROM ranked
),
flagged AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM enriched_payments
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    SIZE(COLLECT_SET(method)) AS num_methods,
    COUNT_IF(method = 'bank_transfer') AS bank_transfer_events,
    ROUND(AVG(running_max_amount), 2) AS avg_running_max_amount,
    PERCENTILE_APPROX(running_max_amount, 0.95) AS running_max_amount_p95
FROM islands
GROUP BY 1
ORDER BY bank_transfer_events;
