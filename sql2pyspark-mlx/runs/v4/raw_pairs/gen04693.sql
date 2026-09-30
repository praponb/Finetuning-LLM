SELECT
    CASE WHEN brand IN ('Umbrella', 'Acme') THEN 'core' ELSE 'other' END AS brand_group,
    category,
    brand,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    MAX(launch_date) AS latest_launch_date,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values
FROM products
WHERE MONTH(launch_date) IN (2, 7, 12)
GROUP BY 1, 2, 3
ORDER BY brand_group DESC, brand DESC;
