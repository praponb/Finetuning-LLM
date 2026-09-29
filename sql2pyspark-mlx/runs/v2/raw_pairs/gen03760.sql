SELECT
    id,
    store_id,
    product_id,
    revenue,
    DATE_SUB(sale_date, 69) AS sale_date_minus_69d
FROM store_sales
WHERE sale_date >= '2024-06-01' AND product_id IN (1, 6, 9, 10);
