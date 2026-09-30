SELECT
    t.id,
    t.is_flagged,
    t.txn_time,
    a.opened_date,
    c.age,
    c.segment
FROM transactions t
JOIN accounts a ON a.id = t.account_id
JOIN customers c ON c.id = a.customer_id
WHERE t.amount BETWEEN 1700 AND 2100;
