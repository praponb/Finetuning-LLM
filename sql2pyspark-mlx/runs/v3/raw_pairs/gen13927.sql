SELECT
    id,
    supplier_id,
    category,
    launch_date,
    product_name,
    stock_qty,
    LAST_VALUE(product_name) OVER (PARTITION BY supplier_id, category ORDER BY launch_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_product_name,
    SUM(stock_qty) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty
FROM products
WHERE cost <= 510
ORDER BY launch_date DESC, running_stock_qty DESC;
