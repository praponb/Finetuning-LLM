SELECT supplier_id, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list, MAX(launch_date) AS last_launch_date FROM products GROUP BY 1;
