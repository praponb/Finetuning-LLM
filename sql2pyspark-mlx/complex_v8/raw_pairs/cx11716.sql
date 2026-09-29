WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        CASE WHEN claimed_amount >= 29336.0 THEN 'large' WHEN claimed_amount >= 20436.5 THEN 'medium' ELSE 'small' END AS claimed_amount_tier,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE (status IS NOT NULL OR policy_id IS NOT NULL)
),
combined_details_adjuster AS (
    -- Enrich with dimension data
    SELECT s.*, a.level AS adjuster_level
    FROM filtered_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.level <= 3
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(approved_amount) OVER w_adjuster_id_policy_id, 2) AS adjuster_id_policy_id_avg_approved_amount,
        SUM(adjuster_level) OVER (PARTITION BY adjuster_level, adjuster_id ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_adjuster_level,
        RANK() OVER w_policy_id_adjuster_level_by AS adjuster_level_rank,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY claimed_amount_tier, policy_id ORDER BY adjuster_level ASC, id))) / 60, 1) AS minutes_since_prev_reported_at,
        ROW_NUMBER() OVER w_claimed_amount_tier_policy_i_2 AS adjuster_level_seq,
        DENSE_RANK() OVER (PARTITION BY adjuster_level, adjuster_id ORDER BY approved_amount ASC) AS approved_amount_dense_rank,
        COUNT(*) OVER (PARTITION BY adjuster_id, policy_id) AS rows_in_adjuster_id_policy_id,
        MAX(approved_amount) OVER (ORDER BY claim_date ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS approved_amount_max_2_prior,
        MIN(adjuster_level) OVER (PARTITION BY adjuster_level) AS adjuster_level_min_adjuster_level,
        SUM(approved_amount) OVER (PARTITION BY adjuster_id, policy_id) AS adjuster_id_policy_id_sum_approved_amount
    FROM combined_details_adjuster
    WINDOW w_adjuster_id_policy_id AS (PARTITION BY adjuster_id, policy_id), w_policy_id_adjuster_level_by AS (PARTITION BY policy_id, adjuster_level ORDER BY adjuster_level), w_claimed_amount_tier_policy_i_2 AS (PARTITION BY claimed_amount_tier, policy_id ORDER BY adjuster_level DESC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claim_date,
        reported_at,
        claimed_amount_tier,
        claim_type_ref,
        adjuster_level,
        adjuster_id_policy_id_avg_approved_amount,
        running_adjuster_level,
        adjuster_level_rank,
        adjuster_level_seq,
        approved_amount_max_2_prior,
        adjuster_level_min_adjuster_level,
        adjuster_id_policy_id_sum_approved_amount,
        MIN(running_adjuster_level) OVER w_reported_at_month_adjuster_i AS reported_at_month_adjuster_id_min_running_adjust,
        NTILE(10) OVER (PARTITION BY claim_type ORDER BY minutes_since_prev_reported_at, id) AS minutes_since_prev_reported_at_decile,
        SUM(adjuster_level_seq) OVER (PARTITION BY MONTH(reported_at), adjuster_id) AS reported_at_month_adjuster_id_sum_adjuster_level,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY claim_type ORDER BY minutes_since_prev_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        ROUND(approved_amount - AVG(approved_amount) OVER w_reported_at_month_adjuster_i, 2) AS approved_amount_vs_reported_at_month_adjuster_id,
        DENSE_RANK() OVER w_claim_type_by_minutes_since AS minutes_since_prev_reported_at_dense_rank,
        ROUND(approved_amount_max_2_prior / NULLIF(SUM(approved_amount_max_2_prior) OVER (PARTITION BY MONTH(reported_at), adjuster_id), 0), 4) AS approved_amount_max_2_prior_share_of_reported_at
    FROM sequenced
    WINDOW w_reported_at_month_adjuster_i AS (PARTITION BY MONTH(reported_at), adjuster_id), w_claim_type_by_minutes_since AS (PARTITION BY claim_type ORDER BY minutes_since_prev_reported_at, id)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    reported_at,
    claimed_amount_tier,
    adjuster_level,
    adjuster_id_policy_id_avg_approved_amount,
    adjuster_level_rank,
    adjuster_level_seq,
    approved_amount_max_2_prior,
    adjuster_level_min_adjuster_level,
    adjuster_id_policy_id_sum_approved_amount,
    reported_at_month_adjuster_id_min_running_adjust,
    minutes_since_prev_reported_at_decile,
    reported_at_month_adjuster_id_sum_adjuster_level,
    second_claim_date,
    minutes_since_prev_reported_at_dense_rank,
    COALESCE(running_adjuster_level, 0) AS running_adjuster_level_filled,
    CASE WHEN adjuster_level_seq >= 8 THEN 'HIGH' WHEN adjuster_level_seq >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS adjuster_level_seq_tier
FROM metrics
ORDER BY adjuster_id_policy_id_sum_approved_amount, adjuster_id DESC, id
LIMIT 10
