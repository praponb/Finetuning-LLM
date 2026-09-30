SELECT
    id,
    category,
    SORT_ARRAY(tags) AS sorted_tags,
    DATE_FORMAT(launch_date, 'yyyy-MM-dd') AS launch_date_label
FROM products
WHERE brand IS NULL AND brand IN ('Initech', 'Globex')
ORDER BY launch_date_label, id
LIMIT 5
