SELECT
    c.id,
    c.segment,
    c.age,
    c.city
FROM customers c
WHERE c.segment IN ('SMB', 'Consumer', 'Corporate') AND NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id)
ORDER BY c.id
