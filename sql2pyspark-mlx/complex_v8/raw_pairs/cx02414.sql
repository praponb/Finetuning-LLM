WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        ABS(premium - 2111.3) AS premium_distance
    FROM policies
    WHERE product = 'AUTO'
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.*,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score,
        p.holder_name
    FROM filtered_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.joined_on BETWEEN DATE '2024-05-23' AND DATE '2025-07-03'
),
enriched_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_filled,
        s.premium_distance,
        s.holder_joined_on,
        s.holder_name,
        p.segment AS holder_segment,
        p.holder_name AS holder_holder_name,
        p.region AS holder_region
    FROM with_parent_holder s
    JOIN policyholders p ON p.id = s.holder_id AND LENGTH(TRIM(p.holder_name)) >= 8
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(status)) AS status_clean,
        LOWER(TRIM(holder_region)) AS holder_region_clean,
        DAYOFWEEK(start_date) AS start_date_dayofweek
    FROM enriched_holder
    WHERE end_date < DATE '2026-08-31'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(premium - AVG(premium) OVER w_status_holder_segment, 2) AS premium_vs_status_holder_segment_avg,
        FIRST_VALUE(premium_filled) OVER (ORDER BY premium_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium_filled,
        COUNT(*) OVER (PARTITION BY status, holder_segment) AS rows_in_status_holder_segment
    FROM enriched
    WINDOW w_status_holder_segment AS (PARTITION BY status, holder_segment)
),
filtered AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.region = 'Central'
        )
)
SELECT
    id,
    holder_id,
    premium,
    start_date,
    end_date,
    premium_filled,
    premium_distance,
    holder_joined_on,
    holder_segment,
    holder_holder_name,
    holder_region,
    status_clean,
    start_date_dayofweek,
    premium_vs_status_holder_segment_avg,
    first_premium_filled,
    rows_in_status_holder_segment,
    ROUND(first_premium_filled / NULLIF(premium, 0), 3) AS first_premium_filled_per_premium,
    CASE WHEN first_premium_filled >= 3560.0 THEN 'large' WHEN first_premium_filled >= 1880.0 THEN 'medium' ELSE 'small' END AS first_premium_filled_tier
FROM filtered
WHERE UPPER(TRIM(status_clean)) LIKE 'E%'
ORDER BY premium, id
LIMIT 5
