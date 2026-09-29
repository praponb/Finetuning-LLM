SELECT id, revenue, sale_date
FROM store_sales
WHERE revenue > (SELECT AVG(revenue) FROM store_sales);
