SELECT
    id,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    supplier_id
FROM products
QUALIFY RANK() OVER (PARTITION BY category ORDER BY cost DESC NULLS LAST) = 1;
