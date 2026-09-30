SELECT *
FROM (
    SELECT
        txn_type,
        merchant,
        amount
    FROM transactions
) src
PIVOT (
    AVG(amount) AS value_sum
    FOR merchant IN ('Amazon' AS amazon, 'Lazada' AS lazada)
);
