WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date < DATE '2025-08-03'
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        ROUND(AVG(premium), 2) AS avg_policy_premium,
        MAX(start_date) AS last_policy_start_date
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.avg_policy_premium,
        p.last_policy_start_date
    FROM filtered_policys s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.avg_policy_premium,
        s.last_policy_start_date,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment
    FROM with_counts s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        status,
        avg_policy_premium,
        last_policy_start_date,
        holder_joined_on,
        holder_segment,
        RANK() OVER (PARTITION BY product ORDER BY avg_policy_premium ASC, id) AS avg_policy_premium_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_holder_joined_on_ties, 3) AS holder_joined_on_percent_rank
    FROM enriched
    WINDOW w_all_by_holder_joined_on_ties AS (ORDER BY holder_joined_on DESC)
)
SELECT *
FROM (
    SELECT holder_id, avg_policy_premium_rank, status, avg_policy_premium
    FROM with_trends
) src
PIVOT (
    MAX(avg_policy_premium)
    FOR status IN ('EXPIRED', 'CANCELLED', 'ACTIVE')
)
ORDER BY holder_id, avg_policy_premium_rank
