SELECT
    id,
    category,
    supplier_id,
    cost,
    launch_date,
    stock_qty,
    LAST_VALUE(launch_date) OVER (PARTITION BY category, supplier_id ORDER BY cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_launch_date,
    LEAD(stock_qty, 1) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) next_stock_qty
FROM products;
