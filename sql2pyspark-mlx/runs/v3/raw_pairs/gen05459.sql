SELECT DATE_TRUNC('MONTH', launch_date) launch_month, SORT_ARRAY(COLLECT_SET(brand)) brand_list FROM products GROUP BY DATE_TRUNC('MONTH', launch_date)
