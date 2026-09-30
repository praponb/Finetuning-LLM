SELECT
    s.customer_id,
    s.country,
    c.credit_limit,
    c.email,
    c.age
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE c.email LIKE '%@gmail.com' AND c.country NOT IN ('UK', 'TH')
ORDER BY customer_id;
