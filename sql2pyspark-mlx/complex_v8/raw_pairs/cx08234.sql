WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM policies
    WHERE product NOT IN ('AUTO', 'HOME')
      AND holder_id >= 3
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MIN(p.end_date) FROM policies p WHERE p.holder_id = s.holder_id) AS first_policy_end_date
    FROM filtered_policys s
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT s.*, p.risk_score AS holder_risk_score
    FROM with_counts s
    JOIN policyholders p ON p.id = s.holder_id
),
with_activity AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_details_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium > 2013.8
              AND p.status = 'ACTIVE'
        )
      AND s.product IN ('HEALTH', 'LIFE', 'HOME')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        status_ref,
        first_policy_end_date,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        FIRST_VALUE(end_date) OVER (ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        LEAD(end_date, 3) OVER (ORDER BY end_date DESC, id) AS next3_end_date,
        DATEDIFF(first_policy_end_date, LAG(first_policy_end_date) OVER (ORDER BY end_date DESC, id)) AS days_since_prev_first_policy_end_date
    FROM with_activity
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.premium_share_of_product,
        s.first_end_date,
        s.days_since_prev_first_policy_end_date,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on
    FROM with_windows s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.premium_share_of_product,
        s.first_end_date,
        s.days_since_prev_first_policy_end_date,
        s.holder_region,
        p.segment AS holder_segment
    FROM combined_details s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    premium,
    status,
    premium_share_of_product,
    days_since_prev_first_policy_end_date,
    holder_region,
    holder_segment,
    LENGTH(TRIM(holder_region)) AS holder_region_len,
    CONCAT_WS('-', status, holder_segment) AS status_holder_segment_label
FROM with_parent_holder
ORDER BY premium, id
LIMIT 5
