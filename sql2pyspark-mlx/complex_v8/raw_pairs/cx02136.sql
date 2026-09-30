WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        CASE WHEN status IN ('CANCELLED', 'LAPSED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label
    FROM policies
    WHERE premium > 2289.0
),
with_details AS (
    -- Join policyholders
    SELECT
        s.*,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment
    FROM prepared s
    JOIN policyholders p ON p.id = s.holder_id
),
qualified AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.start_date >= DATE '2024-07-19'
              AND p.status NOT IN ('CANCELLED', 'ACTIVE')
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(holder_risk_score) OVER (PARTITION BY status_group) AS status_group_min_holder_risk_score,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status_group), 2) AS premium_vs_status_group_avg,
        SUM(holder_risk_score) OVER (ORDER BY start_date NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS holder_risk_score_sum_2
    FROM qualified
),
screened AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.joined_on IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    status_group,
    holder_segment,
    CONCAT(product, '#', CAST(id AS STRING)) AS product_ref
FROM screened
WHERE product IN ('HEALTH', 'LIFE', 'TRAVEL')
ORDER BY product_ref DESC
