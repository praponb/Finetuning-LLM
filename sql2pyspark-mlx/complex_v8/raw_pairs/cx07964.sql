WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE holder_id >= 13
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM prepared s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.*,
        p.segment AS holder_segment_2,
        p.risk_score AS holder_risk_score_2,
        p.joined_on AS holder_joined_on
    FROM joined_holder s
    JOIN policyholders p ON p.id = s.holder_id
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.holder_segment,
        s.holder_segment_2,
        s.holder_risk_score_2,
        s.holder_joined_on,
        p.joined_on AS holder_joined_on_2,
        p.region AS holder_region
    FROM enriched_holder s
    JOIN policyholders p ON p.id = s.holder_id AND p.joined_on < DATE '2025-07-03'
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', holder_joined_on_2) AS holder_joined_on_2_month,
        COUNT(DISTINCT holder_segment) AS distinct_holder_segments,
        ROUND(SUM(holder_risk_score_2), 2) AS total_holder_risk_score_2,
        SUM(CASE WHEN holder_region NOT IN ('West', 'North') THEN 1 ELSE 0 END) AS n_holder_region_not_in_west_no,
        COUNT(*) FILTER (WHERE product IN ('TRAVEL', 'AUTO', 'HEALTH')) AS n_product_in_travel_auto_healt,
        ROUND(AVG(holder_risk_score_2), 2) AS avg_holder_risk_score_2,
        SUM(holder_risk_score_2) FILTER (WHERE holder_segment = 'VIP') AS holder_risk_score_2_holder_segment_vip
    FROM combined_details_holder
    GROUP BY 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_holder_segments, TO_DATE(holder_joined_on_2_month) ORDER BY n_product_in_travel_auto_healt, holder_joined_on_2_month), 3) AS n_product_in_travel_auto_healt_cume_dist,
        ROUND(CUME_DIST() OVER w_distinct_holder_segments_hol_2, 3) AS n_product_in_travel_auto_healt_cume_dist_2,
        MAX(total_holder_risk_score_2) OVER (PARTITION BY distinct_holder_segments) AS distinct_holder_segments_max_total_holder_risk_s
    FROM stats
    WINDOW w_distinct_holder_segments_hol_2 AS (PARTITION BY distinct_holder_segments, TO_DATE(holder_joined_on_2_month) ORDER BY n_product_in_travel_auto_healt)
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM with_trends
    WHERE distinct_holder_segments < (SELECT MAX(distinct_holder_segments) FROM with_trends)
)
-- Final projection
SELECT
    holder_joined_on_2_month,
    total_holder_risk_score_2,
    n_holder_region_not_in_west_no,
    n_product_in_travel_auto_healt,
    n_product_in_travel_auto_healt_cume_dist,
    n_product_in_travel_auto_healt_cume_dist_2,
    distinct_holder_segments_max_total_holder_risk_s,
    COALESCE(avg_holder_risk_score_2, 0.0) AS avg_holder_risk_score_2_filled
FROM screened
