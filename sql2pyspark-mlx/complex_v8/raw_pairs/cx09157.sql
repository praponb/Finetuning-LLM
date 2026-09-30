WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE holder_id <= 17
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status,
        s.status_or_unknown,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score > 0.2
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MIN(premium) AS min_policy_premium,
        COUNT(*) FILTER (WHERE status IN ('CANCELLED', 'ACTIVE')) AS policy_status_count
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.min_policy_premium,
        COALESCE(p.policy_status_count, 0) AS policy_status_count
    FROM joined s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MIN(p.end_date) FROM policies p WHERE p.holder_id = s.holder_id) AS first_policy_end_date
    FROM with_counts s
)
SELECT
    COALESCE(product, 'ALL') AS product,
    holder_id,
    GROUPING_ID() AS gid,
    ROUND(AVG(min_policy_premium), 2) AS avg_min_policy_premium
FROM profiled
GROUP BY CUBE(product, holder_id)
