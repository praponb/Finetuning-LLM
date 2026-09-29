WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
        CASE WHEN approved_amount >= 15477.7 THEN 'large' WHEN approved_amount >= 0.0 THEN 'medium' ELSE 'small' END AS approved_amount_tier,
        UPPER(TRIM(status)) AS status_clean
    FROM claims
    WHERE claim_date >= DATE '2024-06-20'
      AND claim_type IN ('medical', 'fire')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY adjuster_id, approved_amount_tier) AS rows_in_adjuster_id_approved_amount_tier,
        MAX(claimed_amount) OVER (ORDER BY claim_date ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_6_prior,
        NTILE(3) OVER (ORDER BY claim_date ASC, id) AS claim_date_tercile,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_id, approved_amount_tier), 2) AS claimed_amount_vs_adjuster_id_approved_amount_ti
    FROM base
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, claimed_amount_vs_adjuster_id_approved_amount_ti, claimed_amount, claimed_amount_max_6_prior
    FROM ranked
) src
UNPIVOT (
    reading FOR measure IN (claimed_amount_vs_adjuster_id_approved_amount_ti, claimed_amount, claimed_amount_max_6_prior)
) unp
