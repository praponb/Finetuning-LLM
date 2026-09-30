SELECT
    s.id,
    s.browser,
    s.country,
    s.customer_id
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE NOT is_active);
