WITH valid AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE (policy_id <= 27 OR claimed_amount <= 32441.2)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        ROUND(claimed_amount * 0.25, 2) AS claimed_amount_adj
    FROM valid
)
SELECT
    COALESCE(claim_type_or_unknown, 'ALL') AS claim_type_or_unknown,
    policy_id,
    COALESCE(claim_type, 'ALL') AS claim_type,
    GROUPING_ID() AS gid,
    ROUND(SUM(claimed_amount_adj) / NULLIF(SUM(claimed_amount), 0), 3) AS claimed_amount_adj_to_claimed_amount_ratio
FROM annotated
GROUP BY GROUPING SETS ((claim_type_or_unknown, policy_id, claim_type), (claim_type_or_unknown), (policy_id), ())
ORDER BY claim_type_or_unknown NULLS LAST, policy_id NULLS LAST, claim_type NULLS LAST
