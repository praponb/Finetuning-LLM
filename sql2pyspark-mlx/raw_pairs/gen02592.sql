SELECT
    id,
    customer_id,
    balance,
    DATEDIFF(CURRENT_DATE(), opened_date) / 7 AS weeks_since_opened_date
FROM accounts
WHERE account_type IN ('LOAN', 'SAVINGS');
