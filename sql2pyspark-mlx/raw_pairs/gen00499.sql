SELECT DATE_TRUNC('MONTH', opened_date) opened_month, SORT_ARRAY(COLLECT_SET(region)) region_list
FROM stores
WHERE region IN ('North', 'Central', 'East')
GROUP BY DATE_TRUNC('MONTH', opened_date);
