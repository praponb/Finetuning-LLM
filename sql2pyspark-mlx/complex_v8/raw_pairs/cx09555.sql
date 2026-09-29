WITH base_claims AS (
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
        ROUND(approved_amount * 1.5, 2) AS approved_amount_adj,
        DATEDIFF(DATE '2025-06-30', reported_at) AS days_since_reported_at,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE claim_date < DATE '2024-12-02'
      AND claim_type IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(days_since_reported_at - AVG(days_since_reported_at) OVER (PARTITION BY TO_DATE(reported_at)), 2) AS days_since_reported_at_vs_reported_at_to_date_av,
        LAG(claim_date) OVER (PARTITION BY adjuster_id, claim_type ORDER BY claimed_amount DESC NULLS FIRST, id) AS prev_claim_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY greatest_approved_amount_claimed_amount, id), 3) AS greatest_approved_amount_claimed_amount_percent_,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY claim_type, TO_DATE(reported_at) ORDER BY claim_date DESC, id)) AS days_since_prev_claim_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id, claim_type ORDER BY claimed_amount DESC NULLS FIRST, id), 3) AS claimed_amount_percent_rank
    FROM base_claims
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        DATE_FORMAT(prev_claim_date, 'MMM yyyy') AS prev_claim_date_label
    FROM sequenced
    WHERE adjuster_id <= 6
),
with_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.approved_amount_adj,
        s.days_since_reported_at,
        s.greatest_approved_amount_claimed_amount,
        s.days_since_reported_at_vs_reported_at_to_date_av,
        s.prev_claim_date,
        s.greatest_approved_amount_claimed_amount_percent_,
        s.days_since_prev_claim_date,
        s.claimed_amount_percent_rank,
        s.approved_amount_per_claimed_amount,
        s.prev_claim_date_label,
        a.team AS adjuster_team,
        a.adjuster_name,
        p.start_date AS policy_start_date,
        p.status AS policy_status
    FROM enriched s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
latest AS (
    -- Keep the latest row per adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        approved_amount_adj,
        days_since_reported_at,
        greatest_approved_amount_claimed_amount,
        days_since_reported_at_vs_reported_at_to_date_av,
        prev_claim_date,
        greatest_approved_amount_claimed_amount_percent_,
        days_since_prev_claim_date,
        claimed_amount_percent_rank,
        approved_amount_per_claimed_amount,
        prev_claim_date_label,
        adjuster_team,
        adjuster_name,
        policy_start_date,
        policy_status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC, id DESC) AS row_num
        FROM with_details_adjuster
    ) ranked
    WHERE row_num = 1
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(policy_start_date, prev_claim_date) AS days_prev_claim_date_to_policy_start_date,
        DATEDIFF(DATE '2025-12-31', policy_start_date) AS days_since_policy_start_date,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM latest
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_type,
        approved_amount,
        approved_amount_adj,
        days_since_reported_at,
        greatest_approved_amount_claimed_amount,
        prev_claim_date,
        greatest_approved_amount_claimed_amount_percent_,
        claimed_amount_percent_rank,
        approved_amount_per_claimed_amount,
        prev_claim_date_label,
        adjuster_name,
        policy_status,
        claim_type_len,
        RANK() OVER (ORDER BY days_prev_claim_date_to_policy_start_date ASC) AS days_prev_claim_date_to_policy_start_date_rank,
        ROUND(AVG(days_since_policy_start_date) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_days_since_policy_start_date,
        DENSE_RANK() OVER (ORDER BY days_prev_claim_date_to_policy_start_date ASC) AS days_prev_claim_date_to_policy_start_date_dense_
    FROM enriched_2
)
-- Subtotals with CUBE
SELECT
    claim_type_len,
    days_prev_claim_date_to_policy_start_date_rank,
    ROUND(SUM(claimed_amount_percent_rank) / NULLIF(SUM(approved_amount), 0), 3) AS claimed_amount_percent_rank_to_approved_amount_r,
    COUNT(DISTINCT policy_id) AS distinct_policy_ids
FROM ranked
GROUP BY CUBE(claim_type_len, days_prev_claim_date_to_policy_start_date_rank)
