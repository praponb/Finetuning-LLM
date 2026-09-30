SELECT
    id,
    product_id,
    quantity,
    unit_price
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS LAST, id ASC) AS rn
    FROM order_items
) x
WHERE rn = 1;
