SELECT SUM(rating) total_rating, SORT_ARRAY(COLLECT_SET(country)) country_list
FROM suppliers
WHERE (city IN ('London', 'Tokyo') AND supplier_name IS NOT NULL) OR city = 'Singapore'
ORDER BY total_rating
