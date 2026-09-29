WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT_WS('|', product, status) AS product_status_label
    FROM policies
    WHERE (premium <= 3465.4 OR start_date < DATE '2025-02-13')
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM base s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
)
SELECT *
FROM (
    SELECT holder_id, policy_count, product, premium
    FROM profiled
) src
PIVOT (
    SUM(premium)
    FOR product IN ('HOME', 'LIFE')
)
ORDER BY holder_id, policy_count
