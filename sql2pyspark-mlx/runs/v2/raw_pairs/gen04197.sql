SELECT DATE_TRUNC('MONTH', stores.opened_date) AS opened_month, MIN(store_sales.sale_date) AS min_sale_date
FROM store_sales
RIGHT JOIN stores ON store_sales.store_id = stores.id
LEFT JOIN products ON store_sales.product_id = products.id
WHERE store_sales.sale_date BETWEEN '2023-07-01' AND '2023-09-06'
GROUP BY DATE_TRUNC('MONTH', stores.opened_date)
ORDER BY min_sale_date DESC, opened_month ASC
LIMIT 20
