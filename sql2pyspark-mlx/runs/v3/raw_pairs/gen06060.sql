SELECT
    id,
    revenue,
    sale_date,
    DAYOFWEEK(sale_date) sale_date_dayofweek
FROM store_sales
WHERE sale_date > '2024-07-01' AND (revenue BETWEEN 1700 AND 2000 OR units <= 37);
