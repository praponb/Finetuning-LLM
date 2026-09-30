WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount
    FROM claims
),
annotated AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix
    FROM base
    WHERE claim_type IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claim_type_prefix,
        p.premium AS policy_premium,
        po.segment AS holder_segment
    FROM annotated s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
    WHERE p.premium BETWEEN 2033.8 AND 4670.9
)
SELECT
    COALESCE(holder_segment, 'ALL') AS holder_segment,
    COALESCE(claim_type, 'ALL') AS claim_type,
    adjuster_id,
    GROUPING(holder_segment) AS is_total_holder_segment,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM joined
GROUP BY CUBE(holder_segment, claim_type, adjuster_id)
