SELECT DATE_TRUNC('MONTH', ss.sale_date) sale_month, COUNT(i.quantity) quantity_count, ROUND(STDDEV(i.quantity), 2) stddev_quantity
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY DATE_TRUNC('MONTH', ss.sale_date)
ORDER BY sale_month, stddev_quantity DESC;
