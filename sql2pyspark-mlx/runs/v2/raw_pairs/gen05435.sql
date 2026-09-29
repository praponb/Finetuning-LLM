SELECT COUNT(DISTINCT revenue) AS unique_products, ROUND(AVG(units), 2) AS avg_units
FROM store_sales
WHERE revenue < 2800 OR sale_date > '2023-04-02';
