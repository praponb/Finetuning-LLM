SELECT
    *,
    MAX(price) OVER w_part AS category_max_price,
    LEAD(stock_qty, 1) OVER (PARTITION BY supplier_id ORDER BY cost, id) AS next_stock_qty,
    LAST_VALUE(cost, TRUE) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost
FROM products
WINDOW w_part AS (PARTITION BY category);
