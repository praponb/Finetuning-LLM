SELECT
    id,
    supplier_id,
    stock_qty,
    category,
    launch_date,
    LAST_VALUE(category) OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_category,
    NTILE(10) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS launch_date_ntile
FROM products
