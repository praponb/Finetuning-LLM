WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        DATE_ADD(end_date, 90) AS end_date_plus_90d,
        ABS(premium - 2096.6) AS premium_distance,
        INITCAP(TRIM(status)) AS status_clean
    FROM policies
    WHERE start_date BETWEEN DATE '2024-03-15' AND DATE '2025-08-03'
),
status_stats AS (
    -- Avg premium_distance per status
    SELECT status, AVG(premium_distance) AS avg_premium_distance
    FROM prepared
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium_distance,
        ROUND(s.premium_distance - g.avg_premium_distance, 2) AS premium_distance_minus_avg
    FROM prepared s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium_distance > g.avg_premium_distance
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.premium_distance,
        s.status_clean,
        s.avg_premium_distance,
        s.premium_distance_minus_avg,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment,
        p.holder_name
    FROM above_average s
    JOIN policyholders p ON p.id = s.holder_id
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        premium_distance,
        status_clean,
        holder_joined_on,
        holder_segment,
        holder_name,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter
    FROM enriched
    WHERE premium_distance_minus_avg > -1000.0
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    holder_segment,
    ARRAY_SORT(COLLECT_SET(holder_name)) AS holder_name_list
FROM enriched_2
GROUP BY GROUPING SETS ((holder_id, holder_segment), (holder_id), (holder_segment))
ORDER BY holder_id NULLS LAST, holder_segment NULLS LAST
