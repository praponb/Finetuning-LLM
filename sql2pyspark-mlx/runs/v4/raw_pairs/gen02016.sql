WITH cleaned AS (
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        COALESCE(merchant, 'N/A') AS resolved_merchant
    FROM transactions
    WHERE txn_time > DATE '2023-09-13'
      AND amount BETWEEN 2900 AND 3900
),
scored AS (
    SELECT
        *,
        LEAD(amount, 1) OVER (PARTITION BY txn_type, account_id ORDER BY amount, id) AS next_amount,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_sum_30d,
        SUM(amount_filled) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_filled,
        SUM(amount) OVER (ORDER BY amount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM cleaned
),
deduped AS (
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY amount_filled, id) = 1
)
SELECT
    CASE WHEN cumulative_amount_filled >= 375 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_amount_filled_tier,
    CASE WHEN amount >= 3600 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    COUNT(CASE WHEN merchant = 'Lazada' THEN 1 END) AS lazada_count,
    SUM(CASE WHEN txn_type = 'FEE' THEN amount_sum_30d ELSE 0 END) AS fee_amount_sum_30d,
    SUM(amount_sum_30d) FILTER (WHERE merchant = 'Grab') AS grab_total_amount_sum_30d,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    MAX(txn_time) AS last_txn_time,
    PERCENTILE_APPROX(next_amount, 0.95) AS next_amount_p95
FROM deduped
GROUP BY CUBE (CASE WHEN cumulative_amount_filled >= 375 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN amount >= 3600 THEN 'LARGE' ELSE 'SMALL' END)
HAVING grab_total_amount_sum_30d >= 0
ORDER BY fee_amount_sum_30d DESC NULLS LAST
