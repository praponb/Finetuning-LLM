WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        CONCAT_WS(' / ', status, product) AS status_product_label,
        DATE_TRUNC('WEEK', start_date) AS start_date_week,
        DATE_TRUNC('WEEK', end_date) AS end_date_week
    FROM policies
    WHERE product = 'HEALTH'
),
joined_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.status_product_label,
        s.end_date_week,
        p.segment AS holder_segment,
        p.region AS holder_region
    FROM filtered s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.region IN ('Central', 'East', 'South')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_product_label,
        holder_segment,
        holder_region,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(end_date_week), holder_region ORDER BY end_date_week ASC, id), 3) AS end_date_week_cume_dist
    FROM joined_holder
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(end_date_week_cume_dist, 2) OVER (PARTITION BY holder_segment, holder_region ORDER BY end_date_week_cume_dist NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date_week_cume_dist,
        DENSE_RANK() OVER (PARTITION BY holder_segment, holder_region ORDER BY end_date_week_cume_dist NULLS LAST, id) AS end_date_week_cume_dist_dense_rank,
        MIN(end_date_week_cume_dist) OVER w_holder_region AS holder_region_min_end_date_week_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_segment, holder_region ORDER BY end_date_week_cume_dist NULLS LAST), 3) AS end_date_week_cume_dist_percent_rank
    FROM windowed
    WINDOW w_holder_region AS (PARTITION BY holder_region)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(end_date_week_cume_dist) OVER (PARTITION BY holder_region, end_date_week_cume_dist_dense_rank ORDER BY holder_region_min_end_date_week_cume_dist DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date_week_cume_dist,
        COUNT(*) OVER (PARTITION BY holder_segment) AS rows_in_holder_segment
    FROM with_trends
),
without_matches_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM with_windows
    WHERE holder_region_min_end_date_week_cume_dist < (SELECT MAX(holder_region_min_end_date_week_cume_dist) FROM with_windows)
)
SELECT
    id,
    status_product_label,
    holder_segment,
    holder_region,
    second_end_date_week_cume_dist,
    holder_region_min_end_date_week_cume_dist,
    end_date_week_cume_dist_percent_rank,
    first_end_date_week_cume_dist,
    rows_in_holder_segment
FROM without_matches_2
ORDER BY id, holder_region_min_end_date_week_cume_dist DESC
LIMIT 10
