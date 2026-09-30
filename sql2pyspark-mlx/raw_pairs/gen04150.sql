SELECT
    order_id,
    CASE WHEN quantity >= 8 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    COUNT(*) AS row_count,
    COUNT(*) AS row_count_2,
    PERCENTILE_APPROX(quantity, 0.5) AS quantity_p50,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90,
    SUM(quantity) AS total_quantity,
    ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM order_items
GROUP BY order_id, CASE WHEN quantity >= 8 THEN 'LARGE' ELSE 'SMALL' END;
