WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label,
        ABS(approved_amount - 14595.6) AS approved_amount_distance,
        TRUNC(claim_date, 'MM') AS claim_date_month_start
    FROM claims
    WHERE adjuster_id >= 7
),
with_details AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status_claim_type_label,
        s.approved_amount_distance,
        s.claim_date_month_start,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM recent s
    JOIN adjusters a ON a.id = s.adjuster_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(adjuster_level, claimed_amount) AS least_adjuster_level_claimed_amount
    FROM with_details
)
SELECT
    policy_id,
    adjuster_id,
    GROUPING(policy_id) AS is_total_policy_id,
    MAX(claim_date_month_start) AS last_claim_date_month_start
FROM annotated
GROUP BY GROUPING SETS ((policy_id, adjuster_id), (policy_id), (adjuster_id), ())
