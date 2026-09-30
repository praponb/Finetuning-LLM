SELECT store_id, unique_stores, last_sale_date
FROM (
    SELECT store_id, COUNT(DISTINCT revenue) AS unique_stores, MAX(sale_date) AS last_sale_date
    FROM store_sales
    GROUP BY store_id
) AS t
WHERE unique_stores < 6
