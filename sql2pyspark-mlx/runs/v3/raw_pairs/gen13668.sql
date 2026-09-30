SELECT
    status,
    CASE WHEN account_type IN ('LOAN', 'CREDIT') THEN 'tier_1' ELSE 'other' END AS account_type_group,
    CASE WHEN balance >= 67000 THEN 'high' WHEN balance >= 33000 THEN 'mid' ELSE 'low' END AS balance_bucket,
    MIN(opened_date) AS first_opened_date
FROM accounts
WHERE status IN ('OPEN', 'FROZEN', 'CLOSED') AND account_type NOT IN ('CHECKING', 'SAVINGS')
GROUP BY 1, 2, 3
ORDER BY first_opened_date;
