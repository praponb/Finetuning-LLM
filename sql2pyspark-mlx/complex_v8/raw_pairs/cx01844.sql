WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM policies
    WHERE product = 'HOME'
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        p.segment AS holder_segment,
        p.holder_name
    FROM cleaned_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
joined_holder_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.holder_segment,
        p.holder_name AS holder_holder_name,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment_2
    FROM joined_holder s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS min_claim_approved_amount,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS last_claim_reported_at
    FROM joined_holder_2 s
)
-- Subtotals with GROUPING SETS
SELECT
    holder_segment,
    holder_id,
    SUM(CASE WHEN holder_segment NOT IN ('VIP', 'SME') THEN 1 ELSE 0 END) AS n_holder_segment_not_in_vip_sm,
    MAX(holder_risk_score) AS max_holder_risk_score
FROM with_stats
GROUP BY GROUPING SETS ((holder_segment, holder_id), (holder_segment), ())
ORDER BY holder_segment NULLS LAST, holder_id NULLS LAST
