SELECT
    id,
    brand,
    launch_date,
    cost,
    supplier_id,
    price,
    LEAD(cost, 1, 0) OVER (PARTITION BY brand ORDER BY launch_date, id) AS next_cost,
    NTILE(10) OVER (PARTITION BY supplier_id, brand ORDER BY price DESC, id) AS bucket
FROM products
WHERE LOWER(product_name) LIKE '%ate%'
