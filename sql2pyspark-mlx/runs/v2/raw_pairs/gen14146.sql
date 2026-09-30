SELECT MIN(sale_date) AS min_sale_date, COUNT(DISTINCT units) AS unique_products, MIN(revenue) AS min_revenue
FROM store_sales
WHERE sale_date > '2025-05-01';
