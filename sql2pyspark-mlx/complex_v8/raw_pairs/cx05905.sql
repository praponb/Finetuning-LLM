WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref
    FROM policies
    WHERE YEAR(end_date) = 2026
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status, holder_id ORDER BY premium DESC NULLS FIRST, id) AS rn,
        RANK() OVER w_status_by_start_date AS start_date_rank,
        MIN(premium) OVER w_status_product AS status_product_min_premium,
        ROUND(CUME_DIST() OVER w_status_by_premium_ties, 3) AS premium_cume_dist,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_id_product, 0), 4) AS premium_share_of_holder_id_product,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY status, holder_id ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        LAG(premium, 2) OVER w_status_by_premium AS prev2_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, product), 2) AS premium_vs_holder_id_product_avg,
        ROUND(PERCENT_RANK() OVER w_status_by_start_date, 3) AS start_date_percent_rank,
        NTH_VALUE(start_date, 2) OVER (ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date
    FROM scoped_policys
    WINDOW w_status_by_start_date AS (PARTITION BY status ORDER BY start_date, id), w_status_product AS (PARTITION BY status, product), w_status_by_premium_ties AS (PARTITION BY status ORDER BY premium DESC), w_holder_id_product AS (PARTITION BY holder_id, product), w_status_by_premium AS (PARTITION BY status ORDER BY premium DESC, id)
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(second_start_date, 'yyyy-MM') AS second_start_date_ym,
        ROUND(SUM(premium_cume_dist), 2) AS total_premium_cume_dist,
        ROUND(AVG(start_date_rank), 2) AS avg_start_date_rank,
        COUNT(DISTINCT product) AS distinct_products,
        ROUND(100 * COUNT(CASE WHEN status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_status_is_not_null,
        SUM(CASE WHEN product IN ('HEALTH', 'HOME') THEN 1 ELSE 0 END) AS n_product_in_health_home
    FROM metrics
    GROUP BY DATE_FORMAT(second_start_date, 'yyyy-MM')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        second_start_date_ym,
        total_premium_cume_dist,
        avg_start_date_rank,
        pct_status_is_not_null,
        n_product_in_health_home,
        RANK() OVER (PARTITION BY distinct_products ORDER BY n_product_in_health_home) AS n_product_in_health_home_rank,
        MIN(avg_start_date_rank) OVER w_distinct_products_n_product AS distinct_products_n_product_in_health_home_min_a,
        COUNT(*) OVER w_distinct_products_n_product AS rows_in_distinct_products_n_product_in_health_ho,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_products ORDER BY n_product_in_health_home, second_start_date_ym), 3) AS n_product_in_health_home_cume_dist
    FROM per_group
    WINDOW w_distinct_products_n_product AS (PARTITION BY distinct_products, n_product_in_health_home)
)
SELECT
    second_start_date_ym,
    total_premium_cume_dist,
    avg_start_date_rank,
    pct_status_is_not_null,
    n_product_in_health_home,
    n_product_in_health_home_rank,
    distinct_products_n_product_in_health_home_min_a,
    rows_in_distinct_products_n_product_in_health_ho
FROM with_windows
ORDER BY pct_status_is_not_null NULLS LAST
