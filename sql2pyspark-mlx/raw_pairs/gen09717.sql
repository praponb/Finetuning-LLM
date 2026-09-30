WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        LENGTH(status) AS status_length,
        CAST(paid_at AS DATE) AS paid_at_day
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-04-30 12:00:00'
      AND order_id IS NOT NULL
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.status_length,
        o.order_date,
        o.amount AS order_amount
    FROM filtered s
    LEFT JOIN orders o
        ON s.order_id = o.id
    WHERE o.amount > 1570
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(status_length) OVER (PARTITION BY order_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_status_length,
        AVG(status_length) OVER (PARTITION BY order_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_status_length,
        MAX(status_length) OVER (PARTITION BY order_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_status_length,
        COUNT(status_length) OVER (PARTITION BY order_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_status_length,
        LAG(order_amount, 1) OVER (ORDER BY order_date, id) AS prev_order_amount
    FROM joined
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.status_length,
        a.order_amount,
        a.last7_min_status_length,
        a.last7_max_status_length,
        a.last7_count_status_length,
        a.prev_order_amount,
        b.status,
        b.channel
    FROM enriched a
    LEFT JOIN orders b
        ON a.order_id = b.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN order_amount >= 1220 THEN 'PREMIUM' ELSE 'STANDARD' END AS order_amount_tier,
    COUNT(*) FILTER (WHERE status = 'RETURNED') AS returned_rows,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    ROUND(SUM(last7_count_status_length) / NULLIF(COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END), 0), 2) AS last7_count_status_length_per_shipped,
    SUM(CASE WHEN channel = 'partner' THEN status_length ELSE 0 END) AS partner_status_length,
    COUNT(DISTINCT order_id) AS distinct_order_ids
FROM enriched_payments
GROUP BY CASE WHEN order_amount >= 1220 THEN 'PREMIUM' ELSE 'STANDARD' END;
