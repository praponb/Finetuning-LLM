SELECT
    id,
    supplier_id,
    launch_date,
    price,
    LEAD(price, 1) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS next_price
FROM products
WHERE supplier_id IN (1, 19, 32)
