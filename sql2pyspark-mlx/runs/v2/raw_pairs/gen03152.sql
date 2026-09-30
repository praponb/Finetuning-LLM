SELECT
    id,
    product_id,
    unit_price,
    quantity
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS row_num
    FROM order_items
    WHERE unit_price >= 230
) AS t
WHERE row_num <= 1
ORDER BY unit_price DESC, id DESC;
