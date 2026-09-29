SELECT
    a.id,
    a.status,
    c.age,
    c.city,
    REGEXP_REPLACE(c.email, ' ', '') AS email_slug
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
WHERE c.country IN ('SG', 'UK') AND c.email IS NOT NULL
ORDER BY age DESC, id DESC
LIMIT 10
