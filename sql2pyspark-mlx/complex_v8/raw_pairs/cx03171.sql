WITH recent AS (
    -- Scope the claims rows
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
        ABS(claimed_amount - 17585.2) AS claimed_amount_distance,
        CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref,
        ADD_MONTHS(claim_date, 3) AS claim_date_plus_3m
    FROM claims
    WHERE claim_type <> 'flood'
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN claimed_amount_distance >= 27500.0 THEN 'HIGH' WHEN claimed_amount_distance >= 22500.0 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_distance_tier,
        LOWER(TRIM(status_ref)) AS status_ref_clean
    FROM recent
    WHERE claimed_amount_distance < 27500.0
)
SELECT
    policy_id,
    adjuster_id,
    claim_type,
    MAX(claimed_amount_distance) AS max_claimed_amount_distance,
    ROUND(SUM(approved_amount) / NULLIF(SUM(claimed_amount_distance), 0), 3) AS approved_amount_to_claimed_amount_distance_ratio,
    SUM(CASE WHEN status IN ('APPROVED', 'REJECTED') THEN 1 ELSE 0 END) AS n_status_in_approved_rejected
FROM enriched
GROUP BY CUBE(policy_id, adjuster_id, claim_type)
ORDER BY policy_id NULLS LAST, adjuster_id NULLS LAST, claim_type NULLS LAST
