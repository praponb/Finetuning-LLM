SELECT
    s.customer_id,
    s.device,
    c.credit_limit,
    c.country
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE UPPER(TRIM(c.email)) = 'BOB.SMITH@YAHOO.COM';
