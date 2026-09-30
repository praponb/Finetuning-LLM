WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date
    FROM policies
    WHERE product = 'LIFE'
      AND end_date BETWEEN DATE '2025-11-20' AND DATE '2026-10-23'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
        UPPER(TRIM(product)) AS product_clean,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix
    FROM scoped_policys
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.product_clean,
        s.product_prefix,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on
    FROM tagged s
    JOIN policyholders p ON p.id = s.holder_id AND p.segment IN ('SME', 'RETAIL')
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.product_clean,
        s.product_prefix,
        s.holder_region,
        s.holder_joined_on,
        p.holder_name,
        p.segment AS holder_segment
    FROM joined_holder s
    JOIN policyholders p ON p.id = s.holder_id
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM enriched_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium > 2289.0
              AND p.end_date BETWEEN DATE '2025-12-05' AND DATE '2027-05-16'
        )
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.holder_id) AS avg_policy_premium,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count
    FROM filtered s
)
SELECT
    id,
    product,
    start_date,
    product_prefix,
    holder_region,
    holder_joined_on,
    holder_name,
    holder_segment,
    policy_count,
    LEAST(avg_policy_premium, policy_count) AS least_avg_policy_premium_policy_count,
    COALESCE(policy_count, 0) AS policy_count_filled
FROM with_counts
ORDER BY holder_segment DESC NULLS FIRST, id
LIMIT 10
