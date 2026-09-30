WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        DATE_ADD(end_date, 90) AS end_date_plus_90d
    FROM policies
    WHERE start_date IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.end_date_plus_90d,
        p.region AS holder_region
    FROM scoped s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.end_date_plus_90d,
        p.holder_name,
        p.region AS holder_region_2,
        p.joined_on AS holder_joined_on
    FROM joined s
    JOIN policyholders p ON p.id = s.holder_id
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type = 'flood'
        )
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', end_date_plus_90d) AS end_date_plus_90d_quarter,
        DATE_ADD(holder_joined_on, 90) AS holder_joined_on_plus_90d,
        LOWER(REGEXP_REPLACE(TRIM(holder_region_2), ' ', '_')) AS holder_region_2_slug
    FROM qualified
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT SUM(p.premium) FROM policies p WHERE p.holder_id = s.holder_id) AS total_policy_premium,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count_2
    FROM scored s
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    status,
    holder_joined_on,
    end_date_plus_90d_quarter,
    holder_joined_on_plus_90d,
    holder_region_2_slug,
    total_policy_premium,
    policy_count_2,
    DATEDIFF(DATE '2026-01-01', holder_joined_on) AS days_since_holder_joined_on
FROM with_counts
ORDER BY policy_count_2 DESC
