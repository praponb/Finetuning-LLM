WITH cleaned_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        CONCAT(segment, '#', CAST(id AS STRING)) AS segment_ref,
        COALESCE(segment, 'UNKNOWN') AS segment_or_unknown,
        COALESCE(risk_score, 0.0) AS risk_score_filled
    FROM policyholders
),
latest AS (
    -- Keep the latest row per segment_or_unknown
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        segment_ref,
        segment_or_unknown,
        risk_score_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY segment_or_unknown ORDER BY risk_score DESC, id DESC) AS rn
        FROM cleaned_holders
    ) ranked
    WHERE rn = 1
),
with_children_policys AS (
    -- One row per policy
    SELECT
        s.id,
        s.holder_name,
        s.risk_score,
        s.segment_ref,
        p.id AS policy_id,
        p.product AS policy_product,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date,
        p.status AS policy_status
    FROM latest s
    JOIN policies p ON p.holder_id = s.id
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.risk_score,
        s.segment_ref,
        s.policy_id,
        s.policy_product,
        s.policy_end_date,
        s.policy_start_date,
        s.policy_status,
        p.start_date AS policy_start_date_2,
        p.product AS policy_product_2
    FROM with_children_policys s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.start_date >= DATE '2024-07-19'
),
line_level AS (
    -- One row per policy
    SELECT
        s.id,
        s.policy_id,
        s.policy_product,
        s.policy_end_date,
        s.policy_start_date,
        s.policy_status,
        p.id AS policy_id_2,
        p.end_date AS policy_end_date_2,
        p.product AS policy_product_3,
        p.status AS policy_status_2,
        p.start_date AS policy_start_date_3
    FROM combined_details s
    JOIN policies p ON p.holder_id = s.id
),
summary AS (
    -- Group-level totals
    SELECT
        policy_id,
        policy_product_3,
        ROUND(100 * COUNT(CASE WHEN policy_status <> 'CANCELLED' THEN 1 END) / COUNT(*), 1) AS pct_policy_status_cancelled,
        COUNT(DISTINCT policy_id_2) AS distinct_policy_id_2s,
        MIN(policy_end_date) AS first_policy_end_date,
        COUNT(*) AS row_count,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        COUNT(DISTINCT policy_product_3) AS distinct_policy_product_3s
    FROM line_level
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.policy_id,
        s.policy_product_3,
        s.pct_policy_status_cancelled,
        s.distinct_policy_id_2s,
        s.first_policy_end_date,
        s.distinct_policy_ids,
        s.distinct_policy_product_3s,
        p.holder_id AS policy_holder_id,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM summary s
    JOIN policies p ON p.id = s.policy_id
)
SELECT policy_id, policy_product_3, metric, metric_value
FROM (
    SELECT policy_id, policy_product_3, policy_premium, pct_policy_status_cancelled
    FROM combined_details_policy
) src
UNPIVOT (
    metric_value FOR metric IN (policy_premium AS policy_premium_v, pct_policy_status_cancelled AS m_pct_policy_status_cancelled)
) unp
