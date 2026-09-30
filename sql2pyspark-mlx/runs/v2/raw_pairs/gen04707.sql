SELECT a.id, a.account_type, t.7_eleven_amount
FROM accounts a
LEFT JOIN (SELECT account_id, SUM(CASE WHEN merchant = '7-Eleven' THEN amount ELSE 0 END) 7_eleven_amount
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.7_eleven_amount > 44
ORDER BY t.7_eleven_amount;
