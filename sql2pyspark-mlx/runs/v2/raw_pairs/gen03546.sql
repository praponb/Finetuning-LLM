WITH base AS (
    SELECT *
    FROM orders
    WHERE shipping_fee BETWEEN 16 AND 18 AND channel IN ('web', 'partner') AND store_id = 10
),
orders_stats AS (
    SELECT QUARTER(order_date) AS order_quarter, COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END) AS shipped_count, MIN(discount) AS min_discount
    FROM base
    GROUP BY QUARTER(order_date)
)
SELECT order_quarter, shipped_count, min_discount
FROM orders_stats
WHERE shipped_count > 4
