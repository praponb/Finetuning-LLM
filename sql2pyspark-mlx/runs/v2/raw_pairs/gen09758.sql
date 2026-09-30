SELECT id, country, INITCAP(TRIM(name)) name_title
FROM customers
WHERE city = 'Singapore' AND NOT is_active;
