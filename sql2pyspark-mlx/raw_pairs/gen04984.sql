SELECT
    id,
    product_id,
    CAST(revenue AS INT) AS revenue_int,
    DAYOFWEEK(sale_date) AS sale_date_dayofweek
FROM store_sales
WHERE store_id IN (2, 4, 6) AND product_id IN (6, 7, 9, 10);
