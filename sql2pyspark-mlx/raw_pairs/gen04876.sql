SELECT
    CASE WHEN region IN ('Central', 'North') THEN 'tier_1' ELSE 'other' END AS region_group,
    ROUND((100 * SUM(CASE WHEN region = 'North' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_north,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft
FROM stores
WHERE region = 'South'
GROUP BY 1
ORDER BY pct_north DESC;
