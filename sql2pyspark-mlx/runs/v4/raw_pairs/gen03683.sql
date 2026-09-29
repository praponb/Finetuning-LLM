SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    supplier_id,
    LAST_VALUE(brand, TRUE) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_brand,
    LEAD(stock_qty, 1) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id) AS next_stock_qty
FROM products
