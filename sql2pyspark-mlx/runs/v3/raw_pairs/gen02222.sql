SELECT supplier_id, CASE WHEN cost >= 650 THEN 'high' WHEN cost >= 130 THEN 'mid' ELSE 'low' END AS cost_bucket, SUM(stock_qty) AS total_stock_qty
FROM products
WHERE brand IN ('Acme', 'Initech') AND (category = 'Fashion' OR category IN ('Fashion', 'Sports', 'Grocery'))
GROUP BY 1, 2
HAVING SUM(stock_qty) < 1100
ORDER BY total_stock_qty DESC, supplier_id DESC, cost_bucket
LIMIT 10;
