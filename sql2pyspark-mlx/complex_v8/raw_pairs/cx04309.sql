WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE holder_id <= 3
      AND YEAR(start_date) = 2024
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status,
        p.holder_name,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        status,
        ROUND(CUME_DIST() OVER w_status_holder_joined_on_year, 3) AS holder_joined_on_cume_dist,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY status ORDER BY premium DESC, id)) AS days_since_prev_end_date,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY premium DESC, id), 3) AS premium_cume_dist,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product)) AS distinct_holder_id_in_product,
        ROUND(AVG(premium) OVER (PARTITION BY holder_region, YEAR(end_date)), 2) AS holder_region_end_date_year_avg_premium,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_region, YEAR(end_date)), 2) AS premium_vs_holder_region_end_date_year_avg,
        ROUND(PERCENT_RANK() OVER w_status_holder_joined_on_year_2, 3) AS holder_joined_on_percent_rank,
        NTH_VALUE(premium, 2) OVER (PARTITION BY status, YEAR(holder_joined_on) ORDER BY holder_joined_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date_2,
        MIN(premium) OVER (PARTITION BY holder_region, YEAR(end_date)) AS holder_region_end_date_year_min_premium,
        NTH_VALUE(premium, 2) OVER (PARTITION BY status ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_2
    FROM with_parent_holder
    WINDOW w_status_holder_joined_on_year AS (PARTITION BY status, YEAR(holder_joined_on) ORDER BY holder_joined_on DESC NULLS FIRST, id), w_status_holder_joined_on_year_2 AS (PARTITION BY status, YEAR(holder_joined_on) ORDER BY holder_joined_on DESC NULLS FIRST)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id, distinct_holder_id_in_product ORDER BY rows_in_product, id), 3) AS rows_in_product_cume_dist,
        DENSE_RANK() OVER w_holder_id_distinct_holder_id AS rows_in_product_dense_rank,
        ROUND(second_premium - AVG(second_premium) OVER (PARTITION BY distinct_holder_id_in_product), 2) AS second_premium_vs_distinct_holder_id_in_product_,
        LEAD(holder_joined_on_cume_dist) OVER w_status_by_premium_vs_holder AS next_holder_joined_on_cume_dist,
        DENSE_RANK() OVER (PARTITION BY holder_id, distinct_holder_id_in_product ORDER BY rows_in_product) AS rows_in_product_dense_rank_2,
        LAST_VALUE(days_since_prev_end_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY premium_vs_holder_region_end_date_year_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_prev_end_date
    FROM metrics
    WINDOW w_holder_id_distinct_holder_id AS (PARTITION BY holder_id, distinct_holder_id_in_product ORDER BY rows_in_product, id), w_status_by_premium_vs_holder AS (PARTITION BY status ORDER BY premium_vs_holder_region_end_date_year_avg DESC, id)
)
-- Final projection
SELECT DISTINCT next_holder_joined_on_cume_dist, id, rows_in_product
FROM with_trends
ORDER BY rows_in_product DESC, id
