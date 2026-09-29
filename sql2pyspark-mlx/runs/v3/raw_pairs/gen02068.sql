SELECT ss.store_id, YEAR(ss.sale_date) sale_year, MAX(i.last_restock) last_last_restock
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue BETWEEN 1100 AND 1900
GROUP BY ss.store_id, YEAR(ss.sale_date)
ORDER BY sale_year DESC, store_id
LIMIT 5;
