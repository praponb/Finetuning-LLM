WITH scored AS (
    SELECT
        id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_sum_90d,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant, account_id), 4) AS amount_share,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share_2,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        AVG(amount) OVER w_grp AS amount_avg_7d
    FROM transactions
    WHERE merchant = 'Lazada'
    WINDOW w_grp AS (PARTITION BY MONTH(txn_time), account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT
        *,
        CAST(running_amount AS STRING) AS running_amount_num,
        COALESCE(merchant, 'OTHER') AS merchant_clean
    FROM scored
    WHERE is_flagged
),
with_windows AS (
    SELECT
        *,
        MAX(amount_share_2) OVER w_grp AS running_max_amount_share_2,
        SUM(amount_share_2) OVER w_grp AS running_sum_amount_share_2,
        MIN(amount_share_2) OVER w_grp AS running_min_amount_share_2,
        AVG(amount) OVER (PARTITION BY txn_type, merchant ORDER BY amount_avg_7d, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_transactions AS (
    SELECT
        *,
        SUM(amount_share) OVER (PARTITION BY txn_type ORDER BY running_sum_amount_share_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_amount_share,
        COUNT(amount_share) OVER (PARTITION BY txn_type ORDER BY running_sum_amount_share_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_amount_share,
        MAX(amount_share) OVER (PARTITION BY txn_type ORDER BY running_sum_amount_share_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_amount_share,
        MIN(amount_share) OVER (PARTITION BY txn_type ORDER BY running_sum_amount_share_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_amount_share
    FROM with_windows
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY last4_min_amount_share DESC NULLS LAST, id) AS row_num
        FROM scored_transactions
    ) t
    WHERE row_num = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY running_min_amount_share_2, id) AS rank_in_group
        FROM latest
    ) ranked
    WHERE rank_in_group = 1
),
totals AS (
    SELECT
        merchant,
        SIZE(COLLECT_SET(merchant)) AS num_merchants,
        PERCENTILE_APPROX(running_amount, 0.9) AS running_amount_p90,
        SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
        AVG(amount_avg_7d) FILTER (WHERE merchant = 'Starbucks') AS starbucks_avg_amount_avg_7d
    FROM ranked
    GROUP BY merchant
)
SELECT
    merchant,
    num_merchants,
    txn_type_list
FROM totals
ORDER BY num_merchants DESC;
