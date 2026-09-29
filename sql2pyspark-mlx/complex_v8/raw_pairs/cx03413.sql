WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date,
        CASE WHEN status IN ('LAPSED', 'ACTIVE') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group,
        CASE WHEN premium >= 3544.1 THEN 'A' WHEN premium >= 2111.3 THEN 'B' ELSE 'C' END AS premium_tier
    FROM policies
    WHERE end_date < DATE '2026-11-25'
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.days_since_start_date,
        s.status_group,
        s.premium_tier,
        p.segment AS holder_segment,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM prepared s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score > 0.2
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        end_date,
        status_group,
        premium_tier,
        holder_segment,
        LEAST(days_since_start_date, holder_risk_score) AS least_days_since_start_date_holder_risk_score,
        LENGTH(TRIM(product)) AS product_len
    FROM with_details
)
-- Final projection
SELECT
    id,
    holder_id,
    end_date,
    status_group,
    premium_tier,
    product_len,
    DAYOFWEEK(end_date) AS end_date_dayofweek
FROM labeled
WHERE end_date >= DATE '2025-12-05'
ORDER BY product_len DESC NULLS FIRST
