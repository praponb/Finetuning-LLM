SELECT
    id,
    cost,
    supplier_id,
    category,
    launch_date,
    price,
    FIRST_VALUE(supplier_id) OVER (ORDER BY cost DESC, id) AS first_supplier_id,
    price - LAG(price, 1) OVER (PARTITION BY category ORDER BY launch_date, id) AS price_change
FROM products
