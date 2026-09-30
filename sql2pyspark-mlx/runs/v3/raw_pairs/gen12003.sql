SELECT
    accounts.balance,
    accounts.account_type,
    transactions.is_flagged,
    transactions.merchant,
    transactions.amount
FROM accounts
LEFT OUTER JOIN transactions ON accounts.id = transactions.account_id;
