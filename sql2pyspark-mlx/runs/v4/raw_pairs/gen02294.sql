SELECT
    CASE WHEN cost >= 730 THEN 'high' WHEN cost >= 270 THEN 'mid' ELSE 'low' END AS cost_bucket,
    SUM(stock_qty) AS total_stock_qty,
    COUNT(*) AS cnt,
    SUM(SUM(stock_qty)) OVER (ORDER BY CASE WHEN cost >= 730 THEN 'high' WHEN cost >= 270 THEN 'mid' ELSE 'low' END) AS cumulative_stock_qty
FROM products
GROUP BY CASE WHEN cost >= 730 THEN 'high' WHEN cost >= 270 THEN 'mid' ELSE 'low' END
ORDER BY cost_bucket;
