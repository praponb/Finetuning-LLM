SELECT a.opened_date, t.amount_count, t.merchant_list
FROM accounts a
JOIN (SELECT account_id, COUNT(amount) AS amount_count, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
    FROM transactions
    WHERE amount <= 2400
    GROUP BY account_id) t ON a.id = t.account_id
