SELECT t.id, t.is_flagged, t.amount
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE customer_id = 12)
ORDER BY amount DESC, id DESC
