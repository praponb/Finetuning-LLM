WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE approved_amount >= 2272.7
      AND claim_date < DATE '2025-04-14'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY least_approved_amount_claimed_amount) AS least_approved_amount_claimed_amount_rank,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(reported_at), MONTH(claim_date) ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(reported_at), MONTH(claim_date) ORDER BY approved_amount DESC, id), 3) AS approved_amount_percent_rank,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY least_approved_amount_claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        ROW_NUMBER() OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY least_approved_amount_claimed_amount ASC, id) AS least_approved_amount_claimed_amount_seq,
        SUM(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY least_approved_amount_claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_claimed_amount,
        NTH_VALUE(reported_at, 2) OVER (ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at
    FROM filtered_claims
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(last_known_reported_at) OVER (PARTITION BY least_approved_amount_claimed_amount_seq, adjuster_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_known_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY least_approved_amount_claimed_amount_seq, adjuster_id ORDER BY claim_date), 3) AS claim_date_percent_rank,
        ROUND(approved_amount - AVG(approved_amount) OVER w_least_approved_amount_claime_3, 2) AS approved_amount_vs_least_approved_amount_claimed,
        COUNT(*) OVER (PARTITION BY least_approved_amount_claimed_amount_seq) AS rows_in_least_approved_amount_claimed_amount_seq
    FROM with_trends
    WINDOW w_least_approved_amount_claime_3 AS (PARTITION BY least_approved_amount_claimed_amount_seq)
),
totals AS (
    -- Summarise per group
    SELECT
        least_approved_amount_claimed_amount_rank,
        DATE_TRUNC('MONTH', second_reported_at) AS second_reported_at_month,
        policy_id,
        ROUND(SUM(approved_amount_percent_rank) / NULLIF(SUM(least_approved_amount_claimed_amount_rank), 0), 3) AS approved_amount_percent_rank_to_least_approved_a,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        COUNT(*) FILTER (WHERE claim_type NOT IN ('fire', 'theft')) AS n_claim_type_not_in_fire_theft
    FROM with_trends_2
    GROUP BY 1, 2, 3
    HAVING SUM(approved_amount_vs_least_approved_amount_claimed) > -25000.0
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(distinct_policy_ids) OVER w_n_claim_type_not_in_fire_the AS n_claim_type_not_in_fire_theft_distinct_policy_i,
        LEAD(distinct_policy_ids) OVER w_all_by_n_claim_type_not_in_f AS next_distinct_policy_ids
    FROM totals
    WINDOW w_n_claim_type_not_in_fire_the AS (PARTITION BY n_claim_type_not_in_fire_theft, distinct_policy_ids), w_all_by_n_claim_type_not_in_f AS (ORDER BY n_claim_type_not_in_fire_theft ASC, least_approved_amount_claimed_amount_rank, second_reported_at_month, policy_id)
)
-- Final projection
SELECT
    least_approved_amount_claimed_amount_rank,
    second_reported_at_month,
    policy_id,
    approved_amount_percent_rank_to_least_approved_a,
    distinct_policy_ids,
    n_claim_type_not_in_fire_theft,
    n_claim_type_not_in_fire_theft_distinct_policy_i,
    next_distinct_policy_ids
FROM metrics
WHERE n_claim_type_not_in_fire_theft_distinct_policy_i > 11
