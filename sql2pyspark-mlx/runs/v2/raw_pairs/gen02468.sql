SELECT
    id,
    store_id,
    DATE_TRUNC('year', sale_date) AS sale_date_period,
    MONTH(sale_date) AS sale_date_month
FROM store_sales
WHERE (product_id IN (2, 6, 8) AND revenue BETWEEN 1200 AND 1500) OR sale_date <= '2025-10-31';
