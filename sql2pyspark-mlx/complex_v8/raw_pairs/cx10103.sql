WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.status,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status
    FROM prepared s
    JOIN claims c ON c.policy_id = s.id AND c.status = 'IN_REVIEW'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        end_date,
        status,
        claim_id,
        claim_approved_amount,
        SUBSTRING(TRIM(claim_status), 1, 2) AS claim_status_prefix,
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        premium + claim_approved_amount AS premium_plus_claim_approved_amount
    FROM with_children_claims
    WHERE premium > 2013.8
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(claim_approved_amount) OVER (PARTITION BY claim_id, status ORDER BY claim_approved_amount, id, claim_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS claim_approved_amount_avg_2_prior,
        LEAD(end_date, 2) OVER w_claim_id_status_by_claim_app_2 AS next2_end_date,
        NTILE(5) OVER (PARTITION BY claim_id, status ORDER BY claim_approved_amount, id, claim_id) AS claim_approved_amount_quintile,
        ROW_NUMBER() OVER w_claim_id_status_by_claim_app_2 AS claim_approved_amount_seq
    FROM labeled
    WINDOW w_claim_id_status_by_claim_app_2 AS (PARTITION BY claim_id, status ORDER BY claim_approved_amount, id, claim_id)
),
combined_details_claim AS (
    -- Join claims
    SELECT s.*, c.adjuster_id AS claim_adjuster_id
    FROM sequenced s
    JOIN claims c ON c.id = s.claim_id
    WHERE c.adjuster_id <= 4
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.end_date,
        s.status,
        s.claim_id,
        s.claim_approved_amount,
        s.claim_status_prefix,
        s.end_date_month,
        s.premium_plus_claim_approved_amount,
        s.claim_approved_amount_avg_2_prior,
        s.next2_end_date,
        s.claim_approved_amount_quintile,
        s.claim_adjuster_id,
        a.team AS adjuster_team
    FROM combined_details_claim s
    JOIN adjusters a ON a.id = s.claim_adjuster_id
)
SELECT
    id,
    status,
    claim_approved_amount,
    end_date_month,
    premium_plus_claim_approved_amount,
    next2_end_date,
    claim_approved_amount_quintile,
    claim_adjuster_id,
    adjuster_team,
    ROUND(claim_approved_amount * 0.25, 2) AS claim_approved_amount_adj,
    CONCAT(adjuster_team, '#', CAST(id AS STRING)) AS adjuster_team_ref
FROM joined
ORDER BY claim_adjuster_id
