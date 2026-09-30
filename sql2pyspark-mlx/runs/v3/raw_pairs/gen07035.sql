SELECT t.is_flagged, a.status
FROM transactions t
RIGHT JOIN accounts a ON t.account_id = a.id
ORDER BY a.status DESC;
