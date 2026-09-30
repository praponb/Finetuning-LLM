SELECT *, SORT_ARRAY(tags) AS sorted_tags, DATE_SUB(launch_date, 86) AS launch_date_minus_86d
FROM products
WHERE launch_date > '2023-04-01'
