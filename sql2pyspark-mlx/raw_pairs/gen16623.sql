WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        discount
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount, id) AS rank_in_group
        FROM orders
    ) x
    WHERE rank_in_group = 1
)
SELECT
    s.id,
    s.store_id,
    (SELECT SUM(s2.units) FROM store_sales s2 WHERE s2.store_id = s.store_id) AS sum_store_sale_units
FROM ranked s
ORDER BY store_id DESC
