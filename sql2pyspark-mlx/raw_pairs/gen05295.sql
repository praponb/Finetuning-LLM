SELECT COUNT(*) AS num_store_sales, MIN(sale_date) AS min_sale_date
FROM store_sales
WHERE units BETWEEN 14 AND 28;
