WITH base AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        COALESCE(amount, 0) AS amount_filled,
        INITCAP(TRIM(method)) AS method_title,
        COALESCE(method, 'N/A') AS method_clean
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-08-09 20:00:00'
      AND method IS NOT NULL
),
with_dims AS (
    SELECT
        s.id,
        s.order_id,
        s.method,
        s.status,
        s.method_clean,
        p.status AS order_status
    FROM base s
    LEFT JOIN orders p
        ON s.order_id <=> p.id
    WHERE p.order_date > '2025-04-01'
),
valid AS (
    SELECT
        id,
        order_id,
        method,
        status,
        method_clean,
        CONCAT(method, ' ', status) AS method_status
    FROM with_dims
    WHERE order_id IS NOT NULL
),
enriched AS (
    SELECT
        e.id,
        e.order_id,
        b.amount,
        b.discount,
        b.order_date
    FROM valid e
    INNER JOIN orders b
        ON e.order_id = b.id
)
SELECT
    order_id,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    ROUND(AVG(amount), 2) AS avg_amount,
    PERCENTILE_APPROX(amount, 0.5) AS amount_p50,
    ROUND(AVG(amount), 2) AS avg_amount_2,
    GROUPING(order_id) AS grouping_id
FROM enriched
GROUP BY ROLLUP (order_id);
