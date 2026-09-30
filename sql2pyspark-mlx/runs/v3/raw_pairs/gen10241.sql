SELECT
    id,
    age,
    email,
    segment,
    IF(is_active, 'Yes', 'No') AS is_active_label
FROM customers
WHERE name NOT LIKE '%Oliv%' AND age BETWEEN 40 AND 48 AND UPPER(name) LIKE '%ACK%'
ORDER BY segment DESC, age ASC, id ASC
LIMIT 3
