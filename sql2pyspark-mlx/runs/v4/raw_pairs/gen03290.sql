WITH valid AS (
    SELECT *, SUBSTRING(merchant, 1, 2) merchant_prefix
    FROM transactions
    WHERE merchant IS NOT NULL
)
SELECT
    *,
    SUM(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) amount_sum_7d,
    SUM(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) amount_sum_30d,
    RANK() OVER (PARTITION BY account_id ORDER BY amount DESC NULLS LAST) amount_rank,
    CUME_DIST() OVER (PARTITION BY merchant ORDER BY amount NULLS FIRST) amount_cume_dist,
    ROW_NUMBER() OVER (ORDER BY txn_time DESC, id) row_seq
FROM valid;
