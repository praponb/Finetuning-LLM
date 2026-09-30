SELECT *
FROM (
    SELECT
        account_id,
        txn_type,
        amount
    FROM transactions
) src
PIVOT (
    AVG(amount)
    FOR txn_type IN ('DEBIT', 'CREDIT')
);
