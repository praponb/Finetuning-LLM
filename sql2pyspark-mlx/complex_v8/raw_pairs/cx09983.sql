WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        CASE WHEN status IN ('ACTIVE', 'LAPSED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered_policys s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.approved_amount > 2349.3
              AND c.claimed_amount >= 18364.1
        )
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MIN(premium) AS min_policy_premium,
        SUM(premium) AS total_policy_premium
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.min_policy_premium,
        p.total_policy_premium
    FROM filtered s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
)
SELECT id, metric, metric_value
FROM (
    SELECT id, total_policy_premium, min_policy_premium
    FROM summarized_rows
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (total_policy_premium, min_policy_premium)
) unp
