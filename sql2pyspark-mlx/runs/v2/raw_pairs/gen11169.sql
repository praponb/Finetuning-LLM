SELECT QUARTER(ss.sale_date) AS sale_quarter, MAX(i.quantity) AS max_quantity
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY QUARTER(ss.sale_date)
HAVING max_quantity > 60
