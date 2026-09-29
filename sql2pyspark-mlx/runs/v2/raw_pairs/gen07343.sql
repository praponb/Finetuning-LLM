SELECT *
FROM (
    SELECT
        city,
        DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        MIN(sqft) AS min_sqft
    FROM stores
    WHERE sqft > 15478
    GROUP BY city, DATE_FORMAT(opened_date, 'yyyy-MM')
) AS sub
WHERE min_sqft >= 9400;
