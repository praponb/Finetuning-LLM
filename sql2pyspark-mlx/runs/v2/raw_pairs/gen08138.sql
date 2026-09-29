SELECT a.id, a.account_id, a.amount, a.merchant FROM transactions a WHERE a.amount = (SELECT MIN(b.amount) FROM transactions b WHERE b.account_id = a.account_id) ORDER BY a.amount, a.account_id
