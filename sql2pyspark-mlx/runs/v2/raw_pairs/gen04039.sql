SELECT YEAR(opened_date) AS opened_year, region, SORT_ARRAY(COLLECT_SET(city)) AS city_list FROM stores GROUP BY YEAR(opened_date), region
