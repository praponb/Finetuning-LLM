SELECT
    id,
    product_name,
    category,
    price,
    cost,
    stock_qty,
    tags,
    supplier_id,
    AVG(stock_qty) OVER (PARTITION BY MONTH(launch_date), supplier_id ORDER BY launch_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS stock_qty_avg_2,
    NTILE(4) OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC, id) AS stock_qty_bucket
FROM products
