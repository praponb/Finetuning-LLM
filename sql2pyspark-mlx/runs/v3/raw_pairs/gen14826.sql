SELECT
    s.id,
    s.device,
    s.customer_id,
    s.browser
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE country NOT IN ('DE', 'TH'));
