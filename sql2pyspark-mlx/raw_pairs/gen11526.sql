SELECT MONTH(stores.opened_date) AS opened_month, YEAR(stores.opened_date) AS opened_year, ROUND(AVG(store_sales.revenue), 2) AS avg_revenue
FROM store_sales
LEFT JOIN stores ON store_sales.store_id = stores.id
WHERE stores.sqft = 6694
GROUP BY MONTH(stores.opened_date), YEAR(stores.opened_date);
