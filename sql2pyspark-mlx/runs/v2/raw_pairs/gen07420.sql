SELECT
    id,
    age,
    segment,
    CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
FROM customers
WHERE name LIKE '%Mia%' AND is_active
