SELECT
    *,
    COUNT(units) OVER (PARTITION BY product_id) AS product_id_count_units,
    MIN(units) OVER (PARTITION BY DATE_TRUNC('week', sale_date), product_id) AS sale_date_period_min_units
FROM store_sales;
