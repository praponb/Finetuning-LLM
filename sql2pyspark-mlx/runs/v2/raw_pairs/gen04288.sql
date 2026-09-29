SELECT a.id, a.opened_date
FROM accounts a
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.is_flagged = FALSE)
ORDER BY id;
