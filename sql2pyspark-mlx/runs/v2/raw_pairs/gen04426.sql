SELECT
    city,
    MONTH(opened_date) AS opened_month,
    region,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    ROUND((100 * SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_south,
    COUNT(sqft) AS sqft_count
FROM stores
WHERE sqft < 12852 AND city IN ('New York', 'Bangkok', 'Tokyo')
GROUP BY city, MONTH(opened_date), region
