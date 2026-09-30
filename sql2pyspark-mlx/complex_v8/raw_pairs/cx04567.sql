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
        CASE WHEN premium >= 3544.1 THEN 'HIGH' WHEN premium >= 1541.8 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
),
rollup_base AS (
    -- Group-level totals
    SELECT
        holder_id,
        premium_tier,
        YEAR(end_date) AS end_date_year,
        ROUND(100 * COUNT(CASE WHEN product = 'TRAVEL' THEN 1 END) / COUNT(*), 1) AS pct_product_travel,
        COUNT(*) FILTER (WHERE product <> 'AUTO') AS n_product_auto,
        MAX(premium) AS max_premium,
        COUNT(*) AS cnt,
        ROUND(AVG(premium), 2) AS avg_premium
    FROM filtered_policys
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.status NOT IN ('LAPSED', 'CANCELLED') THEN 1 END) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_status_count,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count
    FROM rollup_base s
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, p.joined_on AS holder_joined_on
    FROM with_stats s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on BETWEEN DATE '2024-02-22' AND DATE '2025-09-02'
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MIN(p.start_date) FROM policies p WHERE p.holder_id = s.holder_id) AS first_policy_start_date,
        (SELECT MAX(p.premium) FROM policies p WHERE p.holder_id = s.holder_id) AS max_policy_premium
    FROM joined s
)
SELECT
    holder_id,
    n_product_auto,
    max_premium,
    cnt,
    avg_premium,
    policy_status_count,
    policy_count,
    holder_joined_on,
    first_policy_start_date,
    max_policy_premium,
    ABS(policy_count - 2) AS policy_count_distance
FROM profiled
ORDER BY policy_status_count DESC NULLS FIRST, holder_id DESC NULLS FIRST
