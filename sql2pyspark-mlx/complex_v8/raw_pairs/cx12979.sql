WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd') AS reported_at_label
    FROM claims
    WHERE claimed_amount BETWEEN 4698.6 AND 40639.7
      AND approved_amount >= 0.0
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(reported_at_label, '#', CAST(id AS STRING)) AS reported_at_label_ref
    FROM prepared
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        approved_amount_filled,
        reported_at_label_ref,
        LAG(approved_amount_filled) OVER (PARTITION BY policy_id, claim_type ORDER BY claim_date DESC, id) AS prev_approved_amount_filled,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_id, claim_type), 2) AS claimed_amount_vs_adjuster_id_claim_type_avg,
        RANK() OVER (PARTITION BY policy_id, claim_type ORDER BY claim_date DESC) AS claim_date_rank,
        SUM(approved_amount_filled) OVER (PARTITION BY policy_id, claim_type ORDER BY claim_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_filled_sum_6_prior,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY claim_type ORDER BY approved_amount_filled DESC, id)) AS days_since_prev_claim_date,
        MAX(approved_amount_filled) OVER w_adjuster_id_claim_type AS adjuster_id_claim_type_max_approved_amount_fille
    FROM derived
    WINDOW w_adjuster_id_claim_type AS (PARTITION BY adjuster_id, claim_type)
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.reported_at,
        s.reported_at_label_ref,
        s.prev_approved_amount_filled,
        s.days_since_prev_claim_date,
        s.adjuster_id_claim_type_max_approved_amount_fille,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type AS claim_claim_type
    FROM windowed s
    LEFT JOIN claims c ON c.policy_id = s.policy_id AND c.claim_type NOT IN ('flood', 'medical')
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_id, claim_claim_type, days_since_prev_claim_date
    FROM with_children_claims
) src
PIVOT (
    COUNT(days_since_prev_claim_date)
    FOR claim_claim_type IN ('flood', 'collision', 'medical')
)
ORDER BY claim_id
