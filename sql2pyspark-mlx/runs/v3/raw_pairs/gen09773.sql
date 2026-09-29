SELECT
    DATE_TRUNC('MONTH', a.opened_date) AS opened_month,
    YEAR(a.opened_date) AS opened_year,
    COUNT(t.amount) AS amount_count,
    SUM(CASE WHEN t.txn_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count
FROM transactions t
JOIN accounts a ON a.id = t.account_id
GROUP BY DATE_TRUNC('MONTH', a.opened_date), YEAR(a.opened_date)
ORDER BY amount_count DESC, opened_month;
