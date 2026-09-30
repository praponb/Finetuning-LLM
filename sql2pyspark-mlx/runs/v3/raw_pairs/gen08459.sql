SELECT
    accounts.opened_date,
    accounts.account_type,
    accounts.balance,
    transactions.merchant
FROM accounts
LEFT JOIN transactions ON accounts.id = transactions.account_id;
