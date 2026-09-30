SELECT
    id,
    category,
    product_name,
    CASE WHEN price >= 1260 THEN 'High' WHEN price >= 230 THEN 'Medium' ELSE 'Low' END AS price_tier,
    ADD_MONTHS(launch_date, 12) AS launch_date_plus_months
FROM products
WHERE price > 580 AND LENGTH(product_name) >= 14;
