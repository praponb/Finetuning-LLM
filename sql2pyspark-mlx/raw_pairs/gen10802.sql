WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        CONCAT(status, ' - ', method) AS status_method
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-11-07 19:00:00'
      AND amount <= 1050
),
base_payments AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(method, 'N/A') AS method_filled,
        COALESCE(method, 'N/A') AS resolved_method
    FROM base
    WHERE status = 'FAILED'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.paid_at,
        src.amount_filled,
        src.status_method,
        src.method_filled,
        src.resolved_method,
        d.status AS order_status,
        d.order_date
    FROM base_payments src
    LEFT JOIN orders d
        ON src.order_id = d.id
    WHERE d.store_id IN (11, 31)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(amount_filled) OVER w_grp AS running_max_amount_filled,
        MIN(amount_filled) OVER w_grp AS running_min_amount_filled,
        AVG(amount_filled) OVER w_grp AS running_avg_amount_filled,
        SUM(amount_filled) OVER (PARTITION BY order_status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_filled
    FROM enriched
    WINDOW w_grp AS (PARTITION BY order_id, order_status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    MIN(paid_at) AS first_paid_at,
    SIZE(COLLECT_SET(order_status)) AS num_order_status,
    SIZE(COLLECT_SET(order_status)) AS num_order_status_2
FROM metrics
GROUP BY week_start, activity_month
ORDER BY num_order_status_2 DESC;
