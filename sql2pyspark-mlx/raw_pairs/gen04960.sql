WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        status,
        channel,
        discount,
        shipping_fee
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date, id) AS dedup_rank
        FROM orders
    ) x
    WHERE dedup_rank = 1
)
SELECT
    id,
    customer_id,
    status,
    channel,
    CASE WHEN status RLIKE '^(COMPLETED|SHIPPED)$' THEN 'CORE' ELSE 'PRIORITY' END AS status_zone
FROM ranked
ORDER BY status_zone DESC NULLS LAST, id
