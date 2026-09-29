WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM policies
    WHERE start_date BETWEEN DATE '2024-10-22' AND DATE '2025-03-24'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN premium >= 2945.9 THEN 'A' WHEN premium >= 1541.8 THEN 'B' ELSE 'C' END AS premium_tier
    FROM scoped
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count, MIN(premium) AS min_policy_premium
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count,
        p.min_policy_premium
    FROM labeled s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
)
SELECT id, measure, metric_value
FROM (
    SELECT id, min_policy_premium, premium
    FROM summarized_rows
) src
UNPIVOT (
    metric_value FOR measure IN (min_policy_premium, premium)
) unp
ORDER BY id, measure
