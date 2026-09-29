SELECT MONTH(opened_date) AS opened_month, city, SORT_ARRAY(COLLECT_SET(region)) AS region_list FROM stores GROUP BY MONTH(opened_date), city ORDER BY 1 DESC, 2 DESC LIMIT 5;
