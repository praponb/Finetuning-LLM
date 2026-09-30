SELECT brand, SUM(CASE WHEN brand = 'Stark' THEN stock_qty ELSE 0 END) AS stark_stock_qty, ROUND((100 * SUM(CASE WHEN brand = 'Stark' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_stark
FROM products
WHERE stock_qty < 150
GROUP BY brand
