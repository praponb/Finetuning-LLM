WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label
    FROM claims
    WHERE claimed_amount >= 7204.1
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.approved_amount_per_claimed_amount,
        s.reported_at_label,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_date AS claim_claim_date,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount
    FROM recent s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
),
with_details AS (
    -- Join claims
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.approved_amount_per_claimed_amount,
        s.reported_at_label,
        s.claim_id,
        s.claim_reported_at,
        s.claim_claim_date,
        s.claim_status,
        s.claim_approved_amount,
        c.approved_amount AS claim_approved_amount_2,
        c.reported_at AS claim_reported_at_2,
        c.policy_id AS claim_policy_id,
        a.team AS adjuster_team
    FROM with_children s
    JOIN claims c ON c.id = s.claim_id
    LEFT JOIN adjusters a ON a.id = c.adjuster_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        status,
        reported_at,
        approved_amount_per_claimed_amount,
        reported_at_label,
        claim_id,
        claim_reported_at,
        claim_status,
        claim_approved_amount,
        claim_policy_id,
        SUM(claimed_amount) OVER (PARTITION BY adjuster_id, status) AS adjuster_id_status_sum_claimed_amount,
        MAX(claim_approved_amount_2) OVER (ORDER BY claim_approved_amount_2 ASC, id, claim_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS claim_approved_amount_2_max_7,
        LAST_VALUE(claim_approved_amount) OVER (ORDER BY claim_approved_amount_2 ASC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_approved_amount,
        ROUND(AVG(claim_approved_amount) OVER (PARTITION BY adjuster_id, status), 2) AS adjuster_id_status_avg_claim_approved_amount,
        LAST_VALUE(claim_reported_at_2) IGNORE NULLS OVER (PARTITION BY TO_DATE(reported_at), claim_id ORDER BY reported_at ASC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_reported_at_2,
        NTILE(10) OVER (ORDER BY claim_approved_amount_2 ASC, id, claim_id) AS claim_approved_amount_2_decile
    FROM with_details
)
SELECT
    claim_approved_amount_2_decile,
    claim_id,
    claim_policy_id,
    COUNT(DISTINCT claim_status) AS distinct_claim_status,
    SUM(CASE WHEN status NOT IN ('IN_REVIEW', 'OPEN') THEN 1 ELSE 0 END) AS n_status_not_in_in_review_open
FROM sequenced
GROUP BY ROLLUP(claim_approved_amount_2_decile, claim_id, claim_policy_id)
ORDER BY claim_approved_amount_2_decile NULLS LAST, claim_id NULLS LAST, claim_policy_id NULLS LAST
