SELECT
    id,
    product_id,
    store_id,
    ROUND(revenue, 1) AS revenue_rounded
FROM store_sales
WHERE YEAR(sale_date) = 2024 AND revenue <= 1400;
