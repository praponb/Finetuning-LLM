SELECT a.id, a.opened_date
FROM accounts a
WHERE a.status = 'OPEN' AND a.id IN (SELECT account_id FROM transactions);
