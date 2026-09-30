WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE adjuster_id >= 4
      AND YEAR(claim_date) = 2025
),
with_parent AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        p.start_date AS policy_start_date
    FROM recent_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(approved_amount) OVER (PARTITION BY status ORDER BY policy_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        NTILE(3) OVER w_adjuster_id_claim_type_by_po AS policy_start_date_tercile,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id), 2) AS claimed_amount_vs_claim_date_month_adjuster_id_a,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY policy_start_date) AS policy_start_date_dense_rank,
        MIN(approved_amount) OVER w_claim_type_adjuster_id AS claim_type_adjuster_id_min_approved_amount,
        ROUND(PERCENT_RANK() OVER w_all_by_policy_start_date_tie, 3) AS policy_start_date_percent_rank,
        approved_amount - LAG(approved_amount) OVER w_claim_type_policy_start_date AS approved_amount_change,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type), 2) AS approved_amount_vs_claim_type_avg,
        ROUND(CUME_DIST() OVER (ORDER BY policy_start_date), 3) AS policy_start_date_cume_dist,
        MIN(claimed_amount) OVER (PARTITION BY claim_type, adjuster_id) AS claim_type_adjuster_id_min_claimed_amount,
        DENSE_RANK() OVER w_all_by_approved_amount AS approved_amount_dense_rank
    FROM with_parent
    WINDOW w_adjuster_id_claim_type_by_po AS (PARTITION BY adjuster_id, claim_type ORDER BY policy_start_date DESC, id), w_claim_type_adjuster_id AS (PARTITION BY claim_type, adjuster_id), w_all_by_policy_start_date_tie AS (ORDER BY policy_start_date), w_claim_type_policy_start_date AS (PARTITION BY claim_type, DATE_TRUNC('MONTH', policy_start_date) ORDER BY claim_date DESC, id), w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        policy_start_date,
        first_approved_amount,
        policy_start_date_tercile,
        claimed_amount_vs_claim_date_month_adjuster_id_a,
        claim_type_adjuster_id_min_approved_amount,
        policy_start_date_percent_rank,
        approved_amount_change,
        approved_amount_vs_claim_type_avg,
        policy_start_date_cume_dist,
        claim_type_adjuster_id_min_claimed_amount,
        approved_amount_dense_rank
    FROM ranked
    WHERE policy_start_date_dense_rank <= 2
      AND claim_type_adjuster_id_min_claimed_amount > 125075.0
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    policy_start_date,
    first_approved_amount,
    policy_start_date_tercile,
    claim_type_adjuster_id_min_approved_amount,
    policy_start_date_percent_rank,
    approved_amount_change,
    policy_start_date_cume_dist,
    claim_type_adjuster_id_min_claimed_amount,
    approved_amount_dense_rank,
    claimed_amount + first_approved_amount AS claimed_amount_plus_first_approved_amount,
    ROUND(first_approved_amount / NULLIF(approved_amount_change, 0), 3) AS first_approved_amount_per_approved_amount_change
FROM top_per_group
WHERE approved_amount_change >= -15000.0
ORDER BY claimed_amount DESC NULLS FIRST, id
LIMIT 10
