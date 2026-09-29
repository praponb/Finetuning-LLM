WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        quantity + unit_price AS line_total,
        unit_price + quantity AS line_total_2
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (12, 17, 36)
)
-- Ranking and gap features
SELECT
    id,
    order_id,
    product_id,
    unit_price,
    line_total_2,
    COUNT(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_quantity,
    AVG(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_quantity,
    MAX(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_quantity
FROM base
WHERE unit_price <= 260;
