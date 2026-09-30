WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) row_num
    FROM order_items
)
SELECT id, product_id, unit_price
FROM Numbered
WHERE row_num <= 3
