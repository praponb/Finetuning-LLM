SELECT MAX(rating) max_rating, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) country_values
FROM suppliers
WHERE country IN ('DE', 'US') AND rating <= 2.8
ORDER BY country_values ASC, max_rating DESC;
