SELECT YEAR(opened_date) AS opened_year, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, COUNT(DISTINCT sqft) AS unique_regions
FROM stores
WHERE region IN ('Central', 'South', 'North')
GROUP BY 1
ORDER BY opened_year
