SELECT c.id, c.segment
FROM customers c
WHERE c.city NOT IN ('Singapore', 'Bangkok', 'Chiang Mai') AND NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.account_type IN ('LOAN', 'CREDIT', 'CHECKING'))
