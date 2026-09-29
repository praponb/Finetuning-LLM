SELECT city, SORT_ARRAY(COLLECT_SET(region)) region_list FROM stores GROUP BY city
