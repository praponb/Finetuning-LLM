SELECT
    brand,
    CASE WHEN brand IN ('Globex', 'Stark') THEN 'tier_1' ELSE 'other' END AS brand_group,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values,
    MIN(launch_date) AS first_launch_date,
    SUM(cost) AS total_cost
FROM products
WHERE brand <> 'Umbrella' OR cost IS NULL
GROUP BY 1, 2
ORDER BY first_launch_date DESC, brand;
