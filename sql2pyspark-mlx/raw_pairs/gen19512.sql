WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(txn_type) txn_type_lower,
        TRIM(txn_type) clean_txn_type,
        COALESCE(merchant, 'NONE') merchant_filled
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-08-08 02:00:00'
      AND is_flagged = TRUE
)
-- Ranking and gap features
SELECT
    id,
    account_id,
    amount,
    merchant,
    is_flagged,
    txn_type_lower,
    clean_txn_type,
    COUNT(amount) OVER (PARTITION BY txn_type, account_id ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) amount_count_7d,
    AVG(amount) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) amount_avg_5_prior
FROM filtered;
