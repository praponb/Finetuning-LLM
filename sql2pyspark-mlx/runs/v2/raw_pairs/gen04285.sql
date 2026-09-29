SELECT
    c.segment,
    s.customer_id,
    s.plan,
    CASE WHEN c.country = 'UK' THEN 1 ELSE 0 END AS is_uk
FROM customers c
LEFT JOIN subscriptions s ON s.customer_id = c.id
WHERE c.segment IN ('Corporate', 'SMB', 'Consumer') AND s.plan = 'standard'
