SELECT
    id,
    store_id,
    sale_date,
    units,
    ROUND(units * 0.9, 2) AS adjusted_units,
    ROUND(revenue * 1.15, 2) AS adjusted_revenue
FROM store_sales
WHERE units = 16;
