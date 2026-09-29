WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS rn
    FROM order_items
)
SELECT id, product_id, unit_price
FROM ranked
WHERE rn <= 3
ORDER BY id DESC;
