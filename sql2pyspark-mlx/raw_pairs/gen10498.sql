SELECT
    id,
    price,
    cost,
    supplier_id,
    category,
    launch_date,
    stock_qty,
    LAST_VALUE(cost) OVER (ORDER BY price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_cost,
    LAG(stock_qty, 1) OVER (PARTITION BY supplier_id, category ORDER BY launch_date, id) AS prev_stock_qty
FROM products
ORDER BY price DESC;
