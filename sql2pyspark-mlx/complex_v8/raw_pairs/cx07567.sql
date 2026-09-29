WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN status IN ('ACTIVE', 'EXPIRED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE end_date BETWEEN DATE '2025-06-08' AND DATE '2026-09-19'
),
summary AS (
    -- Group-level totals
    SELECT
        status_group,
        holder_id,
        PERCENTILE_APPROX(premium, 0.5) AS median_premium,
        COUNT(*) AS row_count,
        ROUND(AVG(premium), 2) AS avg_premium,
        MIN(premium) AS min_premium
    FROM base
    GROUP BY status_group, holder_id
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.status_group,
        s.holder_id,
        s.row_count,
        s.avg_premium,
        s.min_premium,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM summary s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    status_group,
    holder_id,
    row_count,
    avg_premium,
    holder_region,
    LENGTH(TRIM(holder_region)) AS holder_region_len
FROM combined_details_holder
ORDER BY row_count NULLS LAST
