SELECT id, price, ADD_MONTHS(launch_date, 6) AS launch_date_plus_months
FROM products
WHERE ARRAY_CONTAINS(tags, 'eco')
