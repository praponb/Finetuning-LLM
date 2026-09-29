WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label,
        ROUND(claimed_amount * 0.9, 2) AS claimed_amount_adj,
        CASE WHEN status IN ('PAID', 'REJECTED') THEN 'group_a' WHEN status = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-06-13' AND DATE '2025-05-30'
      AND policy_id IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT *, TO_DATE(reported_at) AS reported_at_date
    FROM filtered_claims
    WHERE adjuster_id IS NOT NULL
)
-- Subtotals with ROLLUP
SELECT
    adjuster_id,
    status_group,
    policy_id,
    COUNT(DISTINCT status_group) AS distinct_status_groups,
    COUNT(*) AS cnt
FROM enriched
GROUP BY ROLLUP(adjuster_id, status_group, policy_id)
ORDER BY adjuster_id NULLS LAST, status_group NULLS LAST, policy_id NULLS LAST
