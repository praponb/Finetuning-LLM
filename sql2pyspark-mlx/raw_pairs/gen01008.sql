WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY txn_time ASC NULLS LAST, id ASC) AS rank_in_group
    FROM transactions
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        txn_time,
        rank_in_group,
        FIRST_VALUE(merchant, TRUE) OVER (ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant,
        CUME_DIST() OVER (PARTITION BY merchant ORDER BY amount ASC NULLS LAST) AS amount_cume_dist,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share
    FROM deduped
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, rank_in_group / NULLIF(amount_cume_dist, 0) AS rank_in_group_per_amount_cume_dist
    FROM enriched
    WHERE txn_time >= TO_DATE('2023-09-17')
      AND TO_DATE(txn_time) >= '2025-09-01'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        rank_in_group,
        first_known_merchant,
        amount_share,
        rank_in_group_per_amount_cume_dist,
        CEIL(amount_cume_dist) AS amount_cume_dist_whole
    FROM valid
    WHERE txn_time >= DATE '2023-07-17'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        rank_in_group,
        rank_in_group_per_amount_cume_dist,
        amount_cume_dist_whole,
        LAG(amount_share, 2) OVER w_ordered AS prev_amount_share,
        SUM(amount_cume_dist_whole) OVER w_hist AS running_amount_cume_dist_whole,
        DENSE_RANK() OVER (ORDER BY amount_cume_dist_whole ASC) AS amount_cume_dist_whole_dense_rank
    FROM filtered
    WHERE amount_share < 0.32
    WINDOW w_ordered AS (ORDER BY rank_in_group_per_amount_cume_dist ASC, id ASC), w_hist AS (ORDER BY amount_cume_dist_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        rank_in_group_per_amount_cume_dist / NULLIF(prev_amount_share, 0) AS rank_in_group_per_amount_cume_dist_per_prev_amount_share,
        amount_cume_dist_whole + running_amount_cume_dist_whole AS amount_cume_dist_whole_plus_running_amount_cume_dist_whole
    FROM scored
    WHERE rank_in_group_per_amount_cume_dist <= 42
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN rank_in_group_per_amount_cume_dist_per_prev_amount_share >= 76 THEN 'HIGH' ELSE 'LOW' END AS rank_in_group_per_amount_cume_dist_per_prev_amount_share_tier,
    PERCENTILE_APPROX(amount_cume_dist_whole_plus_running_amount_cume_dist_whole, ARRAY(0.25, 0.5, 0.75)) AS amount_cume_dist_whole_plus_running_amount_cume_dist_whole_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    APPROX_PERCENTILE(amount_cume_dist_whole_dense_rank, 0.95) AS amount_cume_dist_whole_dense_rank_p95,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids_3
FROM valid_transactions
GROUP BY CASE WHEN rank_in_group_per_amount_cume_dist_per_prev_amount_share >= 76 THEN 'HIGH' ELSE 'LOW' END
HAVING distinct_ids_2 > 1
ORDER BY distinct_ids_3 ASC
