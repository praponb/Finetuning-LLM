WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ABS(premium - 2033.8) AS premium_distance,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE end_date >= DATE '2026-01-24'
      AND product IN ('HOME', 'AUTO', 'LIFE')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(premium_distance) OVER (PARTITION BY holder_id, MONTH(end_date)) AS holder_id_end_date_month_min_premium_distance,
        NTILE(5) OVER (PARTITION BY product ORDER BY end_date, id) AS end_date_quintile,
        COUNT(*) OVER w_holder_id_end_date_month AS rows_in_holder_id_end_date_month,
        LEAD(premium, 2) OVER (PARTITION BY YEAR(start_date) ORDER BY start_date ASC NULLS LAST, id) AS next2_premium,
        SUM(premium_distance) OVER (PARTITION BY YEAR(start_date) ORDER BY start_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_distance,
        MIN(premium_distance) OVER w_holder_id AS holder_id_min_premium_distance,
        ROUND(premium - AVG(premium) OVER w_holder_id_product, 2) AS premium_vs_holder_id_product_avg,
        SUM(premium_distance) OVER w_holder_id_product AS holder_id_product_sum_premium_distance
    FROM recent_policys
    WINDOW w_holder_id_end_date_month AS (PARTITION BY holder_id, MONTH(end_date)), w_holder_id AS (PARTITION BY holder_id), w_holder_id_product AS (PARTITION BY holder_id, product)
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_holder_id_by_holder_id_produ, 3) AS holder_id_product_sum_premium_distance_cume_dist,
        LEAD(start_date) OVER w_holder_id_by_holder_id_produ AS next_start_date,
        ROUND(next2_premium - AVG(next2_premium) OVER (PARTITION BY holder_id), 2) AS next2_premium_vs_holder_id_avg,
        NTH_VALUE(running_premium_distance, 2) OVER (PARTITION BY YEAR(start_date) ORDER BY next2_premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_premium_distance,
        ROUND(premium_distance / NULLIF(SUM(premium_distance) OVER (PARTITION BY holder_id), 0), 4) AS premium_distance_share_of_holder_id,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY holder_id ORDER BY holder_id_product_sum_premium_distance, id)) AS days_since_prev_end_date,
        ROUND(AVG(end_date_quintile) OVER (PARTITION BY YEAR(start_date) ORDER BY next2_premium DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS end_date_quintile_avg_3_prior
    FROM sequenced
    WINDOW w_holder_id_by_holder_id_produ AS (PARTITION BY holder_id ORDER BY holder_id_product_sum_premium_distance, id)
)
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    status,
    holder_id_end_date_month_min_premium_distance,
    end_date_quintile,
    rows_in_holder_id_end_date_month,
    running_premium_distance,
    holder_id_product_sum_premium_distance_cume_dist,
    next2_premium_vs_holder_id_avg,
    second_running_premium_distance,
    premium_distance_share_of_holder_id,
    days_since_prev_end_date,
    end_date_quintile_avg_3_prior
FROM sequenced_2
ORDER BY running_premium_distance DESC
