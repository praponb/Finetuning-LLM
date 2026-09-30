SELECT
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    region,
    AVG(sqft) AS avg_sqft,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM stores
WHERE region IN ('South', 'East') AND LENGTH(store_name) > 8
GROUP BY DATE_TRUNC('MONTH', opened_date), region
ORDER BY opened_month DESC;
