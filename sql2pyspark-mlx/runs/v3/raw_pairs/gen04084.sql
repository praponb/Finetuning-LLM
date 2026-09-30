SELECT units, store_id, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
FROM store_sales
WHERE revenue > 2100 AND MONTH(sale_date) IN (6, 8, 10)
