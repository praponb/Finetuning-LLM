WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        region,
        risk_score,
        segment,
        DATEDIFF(DATE '2025-06-30', joined_on) AS days_since_joined_on,
        QUARTER(joined_on) AS joined_on_quarter
    FROM policyholders
    WHERE LENGTH(TRIM(holder_name)) >= 5
      AND joined_on IS NOT NULL
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(region)) AS region_len,
        CASE WHEN segment IN ('SME', 'VIP') THEN 'group_a' WHEN segment = 'RETAIL' THEN 'group_b' ELSE 'other' END AS segment_group
    FROM base
    WHERE joined_on_quarter <= 3
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(region)) AS region_clean,
        COALESCE(risk_score, 0.0) AS risk_score_filled,
        ROUND(days_since_joined_on / NULLIF(risk_score, 0), 3) AS days_since_joined_on_per_risk_score
    FROM derived
    WHERE segment IN ('RETAIL', 'SME')
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE p.product <> 'LIFE') FROM policies p WHERE p.holder_id = s.id) AS policy_product_count
    FROM scored s
)
-- Subtotals with ROLLUP
SELECT
    policy_product_count,
    region,
    segment_group,
    ARRAY_SORT(COLLECT_SET(region)) AS region_list,
    ROUND(SUM(days_since_joined_on) / NULLIF(SUM(policy_product_count), 0), 3) AS days_since_joined_on_to_policy_product_count_rat
FROM with_stats
GROUP BY ROLLUP(policy_product_count, region, segment_group)
ORDER BY policy_product_count NULLS LAST, region NULLS LAST, segment_group NULLS LAST
