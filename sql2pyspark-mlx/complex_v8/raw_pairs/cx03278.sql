WITH valid_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE holder_name IS NOT NULL
      AND joined_on BETWEEN DATE '2024-06-18' AND DATE '2025-10-01'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(risk_score) OVER (PARTITION BY segment) AS segment_sum_risk_score,
        COUNT(*) OVER w_segment AS rows_in_segment
    FROM valid_holders
    WINDOW w_segment AS (PARTITION BY segment)
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MAX(start_date) AS last_policy_start_date,
        COUNT(CASE WHEN status <> 'EXPIRED' THEN 1 END) AS policy_status_count,
        MAX(end_date) AS last_policy_end_date
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.last_policy_start_date,
        COALESCE(p.policy_status_count, 0) AS policy_status_count,
        p.last_policy_end_date
    FROM with_windows s
    LEFT JOIN policy_stats p ON p.holder_id = s.id
),
policy_stats_2 AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(CASE WHEN status = 'LAPSED' THEN 1 END) AS policy_status_count_2,
        MAX(premium) AS max_policy_premium,
        COUNT(CASE WHEN product NOT IN ('HEALTH', 'AUTO') THEN 1 END) AS policy_product_count
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_status_count_2, 0) AS policy_status_count_2,
        p.max_policy_premium,
        COALESCE(p.policy_product_count, 0) AS policy_product_count
    FROM summarized_rows s
    LEFT JOIN policy_stats_2 p ON p.holder_id = s.id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(last_policy_start_date, 'yyyy-MM') AS last_policy_start_date_label,
        ABS(policy_status_count_2 - 4) AS policy_status_count_2_distance
    FROM with_stats
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', last_policy_start_date) AS last_policy_start_date_month,
        policy_status_count,
        policy_status_count_2_distance,
        MAX(max_policy_premium) AS max_max_policy_premium,
        MAX(policy_status_count) AS max_policy_status_count,
        ROUND(100 * COUNT(CASE WHEN segment NOT IN ('RETAIL', 'SME') THEN 1 END) / COUNT(*), 1) AS pct_segment_not_in_retail_sme
    FROM scored
    WHERE policy_status_count_2_distance > 2
    GROUP BY 1, 2, 3
    HAVING MAX(segment_sum_risk_score) > 1.0
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ABS(max_policy_status_count - 4) AS max_policy_status_count_distance,
        CASE WHEN max_policy_status_count >= 6 THEN 'HIGH' WHEN max_policy_status_count >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS max_policy_status_count_tier
    FROM summary
)
SELECT
    last_policy_start_date_month,
    policy_status_count,
    policy_status_count_2_distance,
    max_max_policy_premium,
    pct_segment_not_in_retail_sme,
    max_policy_status_count_tier
FROM tagged
ORDER BY policy_status_count_2_distance DESC, pct_segment_not_in_retail_sme DESC
