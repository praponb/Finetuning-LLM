SELECT brand, SUM(CASE WHEN brand = 'Acme' THEN stock_qty ELSE 0 END) AS acme_stock_qty
FROM products
WHERE launch_date <= '2023-11-01'
GROUP BY 1
ORDER BY 1, acme_stock_qty;
