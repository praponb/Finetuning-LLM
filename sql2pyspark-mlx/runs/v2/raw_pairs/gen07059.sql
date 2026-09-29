SELECT
    id,
    category,
    launch_date,
    supplier_id,
    brand,
    stock_qty,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date, id) AS row_num,
    SUM(stock_qty) OVER (PARTITION BY supplier_id, brand ORDER BY launch_date, id) AS running_stock_qty
FROM products
WHERE category = 'Electronics';
