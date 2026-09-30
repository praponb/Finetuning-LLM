SELECT id, sale_date, units
FROM store_sales
WHERE sale_date < (SELECT MIN(sale_date) FROM store_sales)
ORDER BY id DESC;
