SELECT stores.city, MAX(store_sales.revenue) AS max_revenue
FROM store_sales
LEFT JOIN stores ON stores.id = store_sales.store_id
GROUP BY stores.city;
