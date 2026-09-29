WITH valid AS (
    SELECT *, SORT_ARRAY(tags) AS sorted_tags
    FROM products
    WHERE brand IS NOT NULL
      AND stock_qty > 270
)
SELECT
    id,
    product_name,
    category,
    price,
    launch_date,
    tags,
    supplier_id,
    sorted_tags,
    CORR(cost, stock_qty) OVER (PARTITION BY supplier_id, category) AS corr_cost_stock_qty,
    LAG(stock_qty, 1) OVER (PARTITION BY category ORDER BY cost, id) AS prev_stock_qty,
    CORR(price, cost) OVER (PARTITION BY supplier_id) AS corr_price_cost,
    AVG(price) OVER (PARTITION BY supplier_id ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS price_avg_2_prior,
    SUM(stock_qty) OVER (PARTITION BY category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty
FROM valid;
