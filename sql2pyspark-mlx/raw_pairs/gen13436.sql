WITH scored AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        STDDEV_SAMP(amount) OVER (PARTITION BY txn_type) AS txn_type_stddev_samp_amount,
        NTILE(4) OVER (PARTITION BY txn_type, merchant ORDER BY amount, id) AS amount_bucket,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS row_seq,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM transactions
    WHERE txn_type = 'CREDIT'
)
SELECT
    merchant,
    account_id,
    MIN(cumulative_amount) AS min_cumulative_amount,
    COUNT(*) FILTER (WHERE txn_type = 'FEE') AS fee_rows,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    COUNT(*) AS cnt,
    COUNT(*) AS event_count,
    GROUPING(merchant) AS grouping_id
FROM scored
GROUP BY GROUPING SETS ((merchant, account_id), (merchant), ())
ORDER BY min_cumulative_amount NULLS LAST;
