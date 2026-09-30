SELECT *, revenue - units AS revenue_per_unit, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
FROM store_sales
WHERE sale_date >= '2025-04-01';
