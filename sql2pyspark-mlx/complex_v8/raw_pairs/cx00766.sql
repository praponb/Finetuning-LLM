WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM policies
    WHERE end_date BETWEEN DATE '2024-11-08' AND DATE '2026-09-19'
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.start_date) FROM policies p WHERE p.holder_id = s.holder_id) AS last_policy_start_date,
        (SELECT MIN(p.end_date) FROM policies p WHERE p.holder_id = s.holder_id) AS first_policy_end_date,
        (SELECT MIN(p.end_date) FROM policies p WHERE p.holder_id = s.holder_id) AS first_policy_end_date_2
    FROM filtered s
),
with_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.status_prefix,
        s.last_policy_start_date,
        s.first_policy_end_date_2,
        p.segment AS holder_segment,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM summarized_rows s
    JOIN policyholders p ON p.id = s.holder_id AND p.risk_score < 0.6
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        s.status_prefix,
        s.last_policy_start_date,
        s.holder_segment,
        s.holder_region,
        p.risk_score AS holder_risk_score_2,
        p.joined_on AS holder_joined_on,
        p.holder_name
    FROM with_details_holder s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    COALESCE(status, 'ALL') AS status,
    GROUPING(holder_id) AS is_total_holder_id,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids,
    ROUND(SUM(holder_risk_score_2) / NULLIF(SUM(premium), 0), 3) AS holder_risk_score_2_to_premium_ratio,
    COUNT(*) AS row_count
FROM combined_details
GROUP BY GROUPING SETS ((holder_id, status), (holder_id), (status), ())
