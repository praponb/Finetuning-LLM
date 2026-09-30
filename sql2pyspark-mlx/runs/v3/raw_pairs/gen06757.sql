SELECT
    a.id,
    a.customer_id,
    t.credit_amount,
    t.lazada_amount
FROM accounts a
INNER JOIN (SELECT account_id, SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount, SUM(CASE WHEN merchant = 'Lazada' THEN amount ELSE 0 END) AS lazada_amount
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.lazada_amount > 69
ORDER BY id DESC
LIMIT 1;
