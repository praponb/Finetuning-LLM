SELECT DATE_TRUNC('MONTH', opened_date) opened_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) city_values, MAX(sqft) - MIN(sqft) sqft_range FROM stores GROUP BY opened_month
