SELECT
    id,
    supplier_id,
    category,
    launch_date,
    stock_qty,
    FIRST_VALUE(stock_qty) OVER (PARTITION BY supplier_id, category ORDER BY launch_date DESC, id) AS first_stock_qty
FROM products;
