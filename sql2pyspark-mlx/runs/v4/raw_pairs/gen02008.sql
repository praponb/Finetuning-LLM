SELECT
    c.age,
    c.name,
    c.email,
    s.country,
    s.browser,
    UPPER(TRIM(c.name)) AS name_normalized
FROM customers c
LEFT OUTER JOIN sessions s ON c.id = s.customer_id
WHERE s.device IN ('tablet', 'mobile', 'desktop');
