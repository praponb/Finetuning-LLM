SELECT
    id,
    category,
    cost,
    stock_qty,
    brand,
    FIRST_VALUE(stock_qty) OVER (PARTITION BY category ORDER BY cost, id) first_stock_qty,
    ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY cost DESC, id) row_num
FROM products
WHERE launch_date BETWEEN '2024-01-01' AND '2025-06-01';
