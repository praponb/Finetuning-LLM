SELECT region, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values FROM stores WHERE city = 'London' AND opened_date <= '2023-05-30' AND region = 'West' GROUP BY region
