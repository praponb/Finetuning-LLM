SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    status,
    ROUND(AVG(balance), 2) AS avg_balance,
    ROUND(AVG(balance), 2) AS avg_balance_2,
    COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count,
    RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', opened_date) ORDER BY ROUND(AVG(balance), 2) DESC) AS avg_balance_rank
FROM accounts
GROUP BY DATE_TRUNC('QUARTER', opened_date), status
ORDER BY avg_balance DESC
