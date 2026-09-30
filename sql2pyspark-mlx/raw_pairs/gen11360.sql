SELECT QUARTER(sale_date) AS sale_quarter, COUNT(*) AS row_count
FROM store_sales
GROUP BY sale_quarter;
