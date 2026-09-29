SELECT
    *,
    ROUND(cost / SUM(cost) OVER (PARTITION BY brand), 4) AS cost_share,
    LAST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY category, brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
    DENSE_RANK() OVER (PARTITION BY brand ORDER BY cost DESC NULLS LAST) AS cost_dense_rank,
    SUM(stock_qty) OVER (PARTITION BY supplier_id, category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty
FROM products
