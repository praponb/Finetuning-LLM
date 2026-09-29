WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        FLOOR(amount) amount_whole,
        COALESCE(merchant, 'NONE') resolved_merchant
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-04-24 11:00:00'
      AND merchant IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(merchant, TRUE) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_merchant,
        NTILE(3) OVER (PARTITION BY TO_DATE(txn_time) ORDER BY amount_whole DESC, id) amount_whole_bucket,
        LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY MONTH(txn_time), account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id, txn_type), 4) amount_share
    FROM cleaned
    WHERE txn_type != 'FEE'
),
cleaned_transactions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        amount_whole,
        resolved_merchant,
        last_known_amount,
        amount_share,
        ROUND(amount_share * 1.1, 2) amount_share_adjusted,
        CONCAT_WS('-', merchant, txn_type) merchant_txn_type_key
    FROM scored
    WHERE merchant IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned_transactions
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM sessions_transactions
),
sessions_transactions_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps_2
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions_transactions_2
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        amount_share_adjusted,
        is_new_burst,
        AVG(amount_share_adjusted) OVER (PARTITION BY txn_type) txn_type_avg_amount_share_adjusted,
        FIRST_VALUE(amount_whole) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY amount_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_amount_whole,
        SUM(amount_share_adjusted) OVER (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount_share_adjusted,
        LAST_VALUE(is_new_session, TRUE) OVER (PARTITION BY txn_type ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_is_new_session,
        PERCENT_RANK() OVER w_grp is_new_burst_percent_rank
    FROM sessionized
    WINDOW w_grp AS (PARTITION BY txn_type ORDER BY is_new_burst NULLS LAST)
)
-- Grouped metrics
SELECT
    CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_burst_tier,
    CASE WHEN amount >= 3100 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    CASE WHEN last_known_is_new_session >= 1 THEN 'HIGH' ELSE 'LOW' END AS last_known_is_new_session_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(is_new_burst_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS is_new_burst_percent_rank_quartiles,
    PERCENTILE_APPROX(amount_share_adjusted, ARRAY(0.25, 0.5, 0.75)) AS amount_share_adjusted_quartiles,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
FROM enriched
GROUP BY CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN amount >= 3100 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last_known_is_new_session >= 1 THEN 'HIGH' ELSE 'LOW' END
HAVING distinct_ids > 1
ORDER BY amount_tier DESC NULLS FIRST;
