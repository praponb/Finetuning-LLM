SELECT
    a.id,
    a.opened_date,
    c.age,
    c.email,
    c.name,
    UPPER(c.segment) segment_upper
FROM accounts a
LEFT OUTER JOIN customers c ON a.customer_id = c.id
WHERE MONTH(c.signup_date) IN (4, 8, 9)
ORDER BY 6 ASC, c.email ASC
