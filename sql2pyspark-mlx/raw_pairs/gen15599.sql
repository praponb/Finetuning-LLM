WITH cleaned AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END AS is_shell,
        LENGTH(merchant) AS merchant_length,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant
    FROM transactions
    WHERE amount BETWEEN 2700 AND 3800
      AND merchant IS NOT NULL
)
SELECT
    id,
    account_id,
    amount,
    merchant,
    is_shell,
    merchant_length,
    LAG(merchant_length) OVER w_hist AS prev_merchant_length,
    LEAD(is_shell) OVER w_hist AS next_is_shell,
    LAG(is_shell) OVER w_hist AS prev_is_shell,
    ROW_NUMBER() OVER w_hist AS seq_no
FROM cleaned
WINDOW w_hist AS (PARTITION BY merchant ORDER BY is_shell, id);
