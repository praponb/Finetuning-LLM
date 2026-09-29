SELECT a.balance, a.opened_date, t.starbucks_count
FROM accounts a
LEFT OUTER JOIN (SELECT account_id, SUM(CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END) AS starbucks_count
    FROM transactions
    WHERE is_flagged
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.starbucks_count > 41
ORDER BY a.opened_date
