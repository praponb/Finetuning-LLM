SELECT *
FROM (
    SELECT
        status,
        account_type,
        balance
    FROM accounts
) src
PIVOT (
    SUM(balance) AS amount,
    COUNT(1) AS cnt
    FOR account_type IN ('CHECKING', 'CREDIT', 'SAVINGS')
);
