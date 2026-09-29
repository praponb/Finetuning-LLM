WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM policies
    WHERE holder_id >= 7
      AND product = 'TRAVEL'
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM filtered_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_parent_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status = 'EXPIRED'
        )
)
-- Subtotals with GROUPING SETS
SELECT
    product,
    holder_region,
    GROUPING_ID() AS gid,
    ROUND(SUM(premium) / NULLIF(SUM(holder_risk_score), 0), 3) AS premium_to_holder_risk_score_ratio,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids
FROM screened
GROUP BY GROUPING SETS ((product, holder_region), (product), (holder_region), ())
ORDER BY product NULLS LAST, holder_region NULLS LAST
