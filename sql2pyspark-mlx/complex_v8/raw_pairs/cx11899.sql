WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LAST_DAY(claim_date) AS claim_date_month_end,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE approved_amount > 0.0
),
with_parent_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.claim_date_month_end,
        s.claimed_amount_filled,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM valid s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(claimed_amount_filled - 15070.0) AS claimed_amount_filled_distance,
        GREATEST(claimed_amount, claimed_amount_filled) AS greatest_claimed_amount_claimed_amount_filled,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM with_parent_adjuster
)
SELECT *
FROM (
    SELECT adjuster_id, claim_date, adjuster_team, claimed_amount_filled_distance
    FROM derived
) src
PIVOT (
    MAX(claimed_amount_filled_distance) AS max_v, SUM(claimed_amount_filled_distance) AS sum_v
    FOR adjuster_team IN ('Health', 'Travel', 'Property', 'Motor')
)
