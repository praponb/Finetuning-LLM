SELECT
    id,
    product_id,
    unit_price,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS row_num
FROM order_items
WHERE unit_price >= 350
ORDER BY unit_price
