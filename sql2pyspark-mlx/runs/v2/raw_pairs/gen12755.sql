SELECT
    accounts.customer_id,
    accounts.account_type,
    transactions.amount,
    transactions.merchant,
    transactions.account_id,
    CASE WHEN transactions.merchant IN ('Lazada', '7-Eleven') THEN 'Tier 1' ELSE 'Other' END AS merchant_group
FROM accounts
LEFT OUTER JOIN transactions ON transactions.account_id = accounts.id;
