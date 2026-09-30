SELECT
    s.monthly_fee,
    s.start_date,
    c.age,
    c.country,
    c.name,
    REGEXP_EXTRACT(c.email, '@(.+)$', 1) AS email_domain
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.segment = 'SMB' AND c.country IN ('UK', 'US')
