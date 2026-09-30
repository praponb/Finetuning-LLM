WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.1, 2) AS premium_adj,
        CASE WHEN status IN ('LAPSED', 'ACTIVE') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE holder_id >= 9
      AND start_date >= DATE '2024-12-03'
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.premium_adj,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region,
        p.segment AS holder_segment
    FROM filtered s
    JOIN policyholders p ON p.id = s.holder_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(holder_joined_on, 2) OVER (PARTITION BY status, holder_id ORDER BY premium_adj NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_joined_on,
        premium_adj - LAG(premium_adj) OVER (PARTITION BY status, holder_id ORDER BY premium_adj NULLS LAST, id) AS premium_adj_change,
        LAG(premium_adj) OVER (PARTITION BY holder_id, status ORDER BY premium_adj DESC, id) AS prev_premium_adj,
        FIRST_VALUE(premium_adj) IGNORE NULLS OVER (PARTITION BY holder_id, status ORDER BY premium_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_adj,
        ROUND(AVG(premium_adj) OVER (ORDER BY end_date DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_adj_avg_1_prior,
        NTH_VALUE(holder_joined_on, 2) OVER (ORDER BY end_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_joined_on_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, status ORDER BY premium_adj DESC, id), 3) AS premium_adj_percent_rank,
        SUM(premium_adj) OVER (ORDER BY end_date DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_adj_sum_4,
        SUM(premium_adj) OVER (PARTITION BY holder_id, status ORDER BY premium_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_adj,
        LAST_VALUE(premium_adj) IGNORE NULLS OVER (PARTITION BY holder_region ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium_adj,
        ROUND(CUME_DIST() OVER w_holder_id_status_by_premium, 3) AS premium_adj_cume_dist,
        LAG(end_date, 3) OVER (PARTITION BY holder_region ORDER BY end_date, id) AS prev3_end_date,
        FIRST_VALUE(holder_joined_on) IGNORE NULLS OVER (PARTITION BY status, holder_id ORDER BY premium_adj NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_holder_joined_on,
        ROUND(premium_adj / NULLIF(SUM(premium_adj) OVER w_holder_region_end_date_year, 0), 4) AS premium_adj_share_of_holder_region_end_date_year,
        ROUND(CUME_DIST() OVER w_holder_id_status_by_premium_3, 3) AS premium_adj_cume_dist_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY end_date DESC NULLS FIRST), 3) AS end_date_percent_rank
    FROM with_parent
    WINDOW w_holder_id_status_by_premium AS (PARTITION BY holder_id, status ORDER BY premium_adj DESC, id), w_holder_region_end_date_year AS (PARTITION BY holder_region, YEAR(end_date)), w_holder_id_status_by_premium_3 AS (PARTITION BY holder_id, status ORDER BY premium_adj DESC)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        holder_segment,
        second_holder_joined_on,
        premium_adj_change,
        prev_premium_adj,
        first_known_premium_adj,
        second_holder_joined_on_2,
        running_premium_adj,
        last_known_premium_adj,
        premium_adj_cume_dist,
        prev3_end_date,
        first_known_holder_joined_on,
        end_date_percent_rank,
        QUARTER(prev3_end_date) AS prev3_end_date_quarter,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
        COALESCE(running_premium_adj, 0.0) AS running_premium_adj_filled
    FROM ranked
    WHERE prev3_end_date >= DATE '2025-10-12'
)
SELECT
    id,
    holder_id,
    holder_segment,
    premium_adj_change,
    prev_premium_adj,
    second_holder_joined_on_2,
    premium_adj_cume_dist,
    prev3_end_date,
    first_known_holder_joined_on,
    prev3_end_date_quarter,
    running_premium_adj_filled
FROM enriched
