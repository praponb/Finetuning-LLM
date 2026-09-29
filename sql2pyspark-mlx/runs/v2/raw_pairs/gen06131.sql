SELECT DATE_TRUNC('MONTH', ss.sale_date) AS sale_month, MIN(i.last_restock) AS min_last_restock
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units < 21
GROUP BY DATE_TRUNC('MONTH', ss.sale_date)
ORDER BY sale_month DESC, min_last_restock ASC;
