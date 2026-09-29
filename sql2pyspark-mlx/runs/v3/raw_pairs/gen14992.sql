SELECT
    product_id,
    COUNT(*) AS total_count,
    MAX(units) - MIN(units) AS units_range,
    SUM(units) AS total_units
FROM store_sales
GROUP BY product_id;
