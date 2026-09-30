SELECT
    supplier_id,
    PERCENTILE_APPROX(price, 0.9) AS price_p90,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN brand = 'Acme' THEN 1 END), 0), 2) AS stock_qty_per_acme,
    APPROX_PERCENTILE(cost, 0.5) AS cost_p50,
    STDDEV_SAMP(stock_qty) AS stock_qty_stddev,
    CORR(price, stock_qty) AS corr_price_stock_qty
FROM products
GROUP BY supplier_id
ORDER BY stock_qty_per_acme ASC NULLS FIRST;
