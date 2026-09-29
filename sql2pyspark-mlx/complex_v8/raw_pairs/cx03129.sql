WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown_2
    FROM policies
    WHERE holder_id <= 15
      AND status = 'EXPIRED'
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status,
        s.status_or_unknown,
        s.status_or_unknown_2,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on
    FROM filtered_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
without_matches AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM enriched_holder s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.region IS NOT NULL
              AND LOWER(p.holder_name) LIKE '%er%'
        )
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        status_or_unknown_2,
        ROUND(SUM(premium) / NULLIF(SUM(holder_risk_score), 0), 3) AS premium_to_holder_risk_score_ratio,
        ROUND(SUM(premium), 2) AS total_premium,
        COUNT(DISTINCT holder_region) AS distinct_holder_regions,
        MAX(premium) AS max_premium
    FROM without_matches
    GROUP BY 1
),
tagged AS (
    -- Derive helper columns
    SELECT
        status_or_unknown_2,
        total_premium,
        distinct_holder_regions,
        max_premium,
        INITCAP(TRIM(status_or_unknown_2)) AS status_or_unknown_2_clean,
        ABS(distinct_holder_regions - 6) AS distinct_holder_regions_distance,
        GREATEST(total_premium, max_premium) AS greatest_total_premium_max_premium
    FROM summary
),
without_matches_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM tagged
    WHERE max_premium > (SELECT MIN(max_premium) FROM tagged)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(distinct_holder_regions) OVER w_status_or_unknown_2 AS status_or_unknown_2_max_distinct_holder_regions,
        ROUND(max_premium - AVG(max_premium) OVER w_status_or_unknown_2, 2) AS max_premium_vs_status_or_unknown_2_avg,
        FIRST_VALUE(distinct_holder_regions) OVER (PARTITION BY distinct_holder_regions_distance ORDER BY max_premium DESC, status_or_unknown_2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_holder_regions,
        ROUND(distinct_holder_regions - AVG(distinct_holder_regions) OVER (PARTITION BY status_or_unknown_2), 2) AS distinct_holder_regions_vs_status_or_unknown_2_a,
        FIRST_VALUE(max_premium) IGNORE NULLS OVER (PARTITION BY distinct_holder_regions_distance ORDER BY max_premium DESC, status_or_unknown_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_premium,
        ROUND(AVG(total_premium) OVER w_status_or_unknown_2, 2) AS status_or_unknown_2_avg_total_premium
    FROM without_matches_2
    WINDOW w_status_or_unknown_2 AS (PARTITION BY status_or_unknown_2)
)
-- Final projection
SELECT
    status_or_unknown_2,
    total_premium,
    max_premium,
    status_or_unknown_2_clean,
    distinct_holder_regions_distance,
    status_or_unknown_2_max_distinct_holder_regions,
    max_premium_vs_status_or_unknown_2_avg,
    first_distinct_holder_regions,
    distinct_holder_regions_vs_status_or_unknown_2_a,
    status_or_unknown_2_avg_total_premium,
    COALESCE(status_or_unknown_2, 'UNKNOWN') AS status_or_unknown_2_or_unknown,
    GREATEST(total_premium, max_premium) AS greatest_total_premium_max_premium_2
FROM with_windows
ORDER BY distinct_holder_regions_vs_status_or_unknown_2_a DESC NULLS FIRST, total_premium DESC, status_or_unknown_2
LIMIT 20
