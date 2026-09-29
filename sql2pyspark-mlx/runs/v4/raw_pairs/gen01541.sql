WITH base AS (
    SELECT
        *,
        CAST(amount AS INT) AS amount_int,
        LENGTH(merchant) AS merchant_length
    FROM transactions
    WHERE merchant IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY amount_int, id) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
kept_keys AS (
    SELECT merchant
    FROM latest
    EXCEPT ALL
    SELECT merchant
    FROM transactions
    WHERE account_id IN (1, 14, 20, 39)
),
verified AS (
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.merchant = k.merchant
),
joined AS (
    SELECT
        e.*,
        d.balance,
        d.opened_date
    FROM verified e
    INNER JOIN accounts d
        ON e.account_id = d.id
)
SELECT *
FROM (
    SELECT
        account_id,
        amount,
        balance
    FROM joined
) src
UNPIVOT (
    reading FOR metric IN (amount AS AMOUNT_VAL, balance AS BALANCE)
)
