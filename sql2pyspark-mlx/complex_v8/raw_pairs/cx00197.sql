WITH base_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE approved_amount > 7204.1
),
combined_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.claimed_amount_per_approved_amount,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        p.start_date AS policy_start_date,
        po.joined_on AS holder_joined_on
    FROM base_claims s
    JOIN policies p ON p.id = s.policy_id AND p.status IN ('LAPSED', 'EXPIRED')
    JOIN policyholders po ON po.id = p.holder_id
    WHERE p.end_date IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount_per_approved_amount,
        policy_end_date,
        policy_product,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_claim_type_adjuster_id, 0), 4) AS approved_amount_share_of_claim_type_adjuster_id,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        MAX(approved_amount) OVER w_claim_type_adjuster_id AS claim_type_adjuster_id_max_approved_amount,
        ROUND(AVG(approved_amount) OVER (PARTITION BY status ORDER BY claimed_amount_per_approved_amount DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_4,
        MAX(approved_amount) OVER (PARTITION BY status) AS status_max_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY policy_start_date ASC, id) AS rn,
        MAX(approved_amount) OVER (PARTITION BY status ORDER BY claimed_amount_per_approved_amount DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_max_3,
        ROUND(AVG(approved_amount) OVER (PARTITION BY YEAR(claim_date), claim_type ORDER BY policy_start_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_3_prior
    FROM combined_details_policy
    WINDOW w_claim_type_adjuster_id AS (PARTITION BY claim_type, adjuster_id)
),
top_rows AS (
    -- Top 3 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount_per_approved_amount,
        policy_end_date,
        policy_product,
        approved_amount_share_of_claim_type_adjuster_id,
        rows_in_status,
        claim_type_adjuster_id_max_approved_amount,
        approved_amount_avg_4,
        status_max_approved_amount,
        approved_amount_max_3,
        approved_amount_avg_3_prior
    FROM with_windows
    WHERE rn <= 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(rows_in_status) OVER (PARTITION BY policy_product, DATE_TRUNC('MONTH', policy_end_date) ORDER BY approved_amount_avg_3_prior DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rows_in_status_max_3_prior,
        DENSE_RANK() OVER (PARTITION BY policy_product, DATE_TRUNC('MONTH', policy_end_date) ORDER BY approved_amount_avg_3_prior DESC, id) AS approved_amount_avg_3_prior_dense_rank,
        COUNT(*) OVER (PARTITION BY claim_type, policy_id) AS rows_in_claim_type_policy_id,
        ROUND(AVG(status_max_approved_amount) OVER w_claim_type_policy_id, 2) AS claim_type_policy_id_avg_status_max_approved_amo
    FROM top_rows
    WINDOW w_claim_type_policy_id AS (PARTITION BY claim_type, policy_id)
)
SELECT DISTINCT
    status_max_approved_amount,
    id,
    approved_amount_share_of_claim_type_adjuster_id
FROM ranked
ORDER BY approved_amount_share_of_claim_type_adjuster_id DESC NULLS FIRST, id DESC NULLS FIRST
