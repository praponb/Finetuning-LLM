WITH scoped AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment,
        MONTH(joined_on) AS joined_on_month,
        SUBSTRING(TRIM(holder_name), 1, 4) AS holder_name_prefix,
        COALESCE(segment, 'UNKNOWN') AS segment_or_unknown
    FROM policyholders
    WHERE region = 'East'
),
qualified AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.premium > 1042.5
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(joined_on_month) IGNORE NULLS OVER (PARTITION BY segment ORDER BY joined_on_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_joined_on_month,
        ROUND(PERCENT_RANK() OVER w_segment_or_unknown_by_joined, 3) AS joined_on_month_percent_rank,
        RANK() OVER w_region_segment_or_unknown_by AS joined_on_month_rank
    FROM qualified
    WINDOW w_segment_or_unknown_by_joined AS (PARTITION BY segment_or_unknown ORDER BY joined_on_month ASC), w_region_segment_or_unknown_by AS (PARTITION BY region, segment_or_unknown ORDER BY joined_on_month DESC, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        joined_on_month,
        joined_on_month_rank,
        ROUND(SUM(first_known_joined_on_month) / NULLIF(SUM(joined_on_month_percent_rank), 0), 3) AS first_known_joined_on_month_to_joined_on_month_p,
        COUNT(*) AS cnt,
        SUM(CASE WHEN segment = 'SME' THEN 1 ELSE 0 END) AS n_segment_sme
    FROM metrics
    GROUP BY joined_on_month, joined_on_month_rank
    HAVING SUM(joined_on_month) > 4
),
enriched AS (
    -- Add calculated columns
    SELECT
        joined_on_month,
        joined_on_month_rank,
        first_known_joined_on_month_to_joined_on_month_p,
        n_segment_sme,
        ABS(cnt - 8) AS cnt_distance
    FROM grouped
    WHERE joined_on_month >= 5
),
qualified_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM enriched
    WHERE n_segment_sme < (SELECT MAX(n_segment_sme) FROM enriched)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(cnt_distance, joined_on_month) AS least_cnt_distance_joined_on_month
    FROM qualified_2
)
SELECT
    joined_on_month,
    joined_on_month_rank,
    n_segment_sme,
    cnt_distance,
    least_cnt_distance_joined_on_month
FROM tagged
WHERE first_known_joined_on_month_to_joined_on_month_p <= 7.5
ORDER BY joined_on_month_rank DESC, least_cnt_distance_joined_on_month
