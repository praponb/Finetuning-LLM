WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date
    FROM policies
    WHERE end_date BETWEEN DATE '2025-09-27' AND DATE '2026-12-05'
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(CASE WHEN product = 'HOME' THEN 1 END) AS policy_product_count
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_product_count, 0) AS policy_product_count
    FROM scoped_policys s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
)
-- Subtotals with CUBE
SELECT
    policy_product_count,
    COALESCE(status, 'ALL') AS status,
    COUNT(DISTINCT status) AS distinct_status
FROM summarized_rows
GROUP BY CUBE(policy_product_count, status)
ORDER BY policy_product_count NULLS LAST, status NULLS LAST
