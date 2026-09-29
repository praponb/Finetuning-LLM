SELECT
    id,
    name,
    is_active,
    signup_date,
    CASE WHEN country IN ('UK', 'SG') THEN 'Group A' ELSE 'Other' END AS country_group
FROM customers
WHERE name LIKE 'Emm%'
