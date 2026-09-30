SELECT products.brand, COUNT(*) AS store_sale_count
FROM store_sales
RIGHT OUTER JOIN products ON store_sales.product_id = products.id
WHERE store_sales.revenue BETWEEN 2700 AND 3400
GROUP BY products.brand
ORDER BY brand DESC, store_sale_count DESC
