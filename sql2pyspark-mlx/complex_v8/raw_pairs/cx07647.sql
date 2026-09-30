WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 10 AND 16
      AND YEAR(claim_date) = 2024
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_id, status), 0), 4) AS claimed_amount_share_of_adjuster_id_status,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY status, DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at
    FROM cleaned
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN claimed_amount >= 39666.5 THEN 'A' WHEN claimed_amount >= 10518.4 THEN 'B' ELSE 'C' END AS claimed_amount_tier,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label,
        DATE_TRUNC('YEAR', claim_date) AS claim_date_year
    FROM ranked
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(claimed_amount - 27813.3) AS claimed_amount_distance,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM scored
    WHERE claim_date_label IS NOT NULL
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(claimed_amount_distance * 0.25, 2) AS claimed_amount_distance_adj
    FROM labeled
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.reported_at,
        s.claimed_amount_tier,
        s.claimed_amount_distance,
        s.claimed_amount_per_approved_amount,
        s.claimed_amount_distance_adj,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.status AS claim_status,
        c.claim_date AS claim_claim_date,
        c.claimed_amount AS claim_claimed_amount
    FROM tagged s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_claim_date, claimed_amount_tier, claimed_amount_per_approved_amount
    FROM exploded_detail_claims
) src
PIVOT (
    SUM(claimed_amount_per_approved_amount)
    FOR claimed_amount_tier IN ('C', 'A', 'B')
)
