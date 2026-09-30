WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment,
        LENGTH(TRIM(region)) AS region_len
    FROM policyholders
    WHERE UPPER(TRIM(holder_name)) LIKE 'M%'
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE p.status IS NOT NULL) FROM policies p WHERE p.holder_id = s.id) AS policy_status_count,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.id) AS policy_count,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.id) AS policy_count_2
    FROM base s
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM profiled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.premium > 2289.0
              AND p.end_date < DATE '2026-10-23'
        )
),
totals AS (
    -- Summarise per group
    SELECT
        policy_count_2,
        MAX(joined_on) AS last_joined_on,
        ARRAY_SORT(COLLECT_SET(region)) AS region_list,
        DATEDIFF(MAX(joined_on), MIN(joined_on)) AS joined_on_span_days,
        COUNT(DISTINCT region) AS distinct_regions,
        ROUND(AVG(region_len), 2) AS avg_region_len
    FROM screened
    GROUP BY policy_count_2
    HAVING SUM(policy_count) > 1
)
-- Final projection
SELECT
    policy_count_2,
    joined_on_span_days,
    distinct_regions,
    CASE WHEN avg_region_len >= 9.8 THEN 'HIGH' WHEN avg_region_len >= 5.0 THEN 'MEDIUM' ELSE 'LOW' END AS avg_region_len_tier,
    CASE WHEN avg_region_len >= 12.2 THEN 'large' WHEN avg_region_len >= 5.0 THEN 'medium' ELSE 'small' END AS avg_region_len_tier_2
FROM totals
WHERE policy_count_2 < 6
ORDER BY avg_region_len_tier_2
