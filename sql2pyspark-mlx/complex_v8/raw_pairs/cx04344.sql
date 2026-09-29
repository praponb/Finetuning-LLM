WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref
    FROM policies
    WHERE start_date < DATE '2025-03-06'
      AND status IN ('CANCELLED', 'EXPIRED', 'ACTIVE')
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        s.status_ref,
        s.product_ref,
        p.holder_name,
        p.risk_score AS holder_risk_score
    FROM scoped_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.status_ref,
        s.holder_name,
        p.region AS holder_region
    FROM enriched s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    COALESCE(holder_region, 'ALL') AS holder_region,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(100 * COUNT(CASE WHEN holder_region NOT IN ('West', 'South') THEN 1 END) / COUNT(*), 1) AS pct_holder_region_not_in_west_so
FROM joined_holder
GROUP BY GROUPING SETS ((holder_id, holder_region), (holder_id), (holder_region))
ORDER BY holder_id NULLS LAST, holder_region NULLS LAST
