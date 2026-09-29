WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount
    FROM claims
),
derived AS (
    -- Derive helper columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM cleaned
    WHERE approved_amount < 14993.0
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        claim_type_slug,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        ABS(claimed_amount - 18364.1) AS claimed_amount_distance
    FROM derived
    WHERE claimed_amount >= 4698.6
),
joined_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM scored s
    JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    claim_type_slug,
    approved_amount_per_claimed_amount,
    claimed_amount_distance,
    adjuster_level,
    CASE WHEN adjuster_team IN ('Property', 'Motor') THEN 'group_a' WHEN adjuster_team = 'Travel' THEN 'group_b' ELSE 'other' END AS adjuster_team_group,
    ABS(claimed_amount - 19347.2) AS claimed_amount_distance_2
FROM joined_adjuster
ORDER BY claimed_amount_distance_2 DESC
