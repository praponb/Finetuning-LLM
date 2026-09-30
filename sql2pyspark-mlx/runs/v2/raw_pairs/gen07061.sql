SELECT
    c.id,
    c.segment,
    c.credit_limit,
    c.email
FROM customers c
WHERE c.credit_limit >= 13100 AND c.id IN (SELECT customer_id FROM sessions WHERE country NOT IN ('DE', 'US'))
