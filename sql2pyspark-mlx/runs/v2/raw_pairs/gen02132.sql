SELECT
    store_id,
    sale_date,
    product_id,
    DATE_TRUNC('YEAR', sale_date) AS sale_date_period
FROM store_sales
WHERE sale_date BETWEEN '2024-05-01' AND '2025-02-01' AND sale_date BETWEEN '2024-02-01' AND '2025-04-01'
