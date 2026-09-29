WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE status NOT IN ('LAPSED', 'CANCELLED')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), product ORDER BY premium ASC, id) AS premium_rank,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_id)) AS distinct_product_in_holder_id,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product, YEAR(start_date))) AS distinct_holder_id_in_product_start_date_year,
        NTH_VALUE(end_date, 2) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        LEAD(start_date) OVER (PARTITION BY holder_id ORDER BY premium, id) AS next_start_date,
        NTILE(3) OVER w_holder_id_by_premium AS premium_tercile,
        DENSE_RANK() OVER (PARTITION BY holder_id ORDER BY premium) AS premium_dense_rank,
        MAX(premium) OVER (ORDER BY premium NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_max_4,
        ROUND(AVG(premium) OVER (ORDER BY premium NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        FIRST_VALUE(start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), product ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        SUM(premium) OVER (PARTITION BY holder_id) AS holder_id_sum_premium,
        LAG(start_date) OVER w_all_by_premium AS prev_start_date,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY premium), 3) AS premium_cume_dist,
        LEAD(end_date, 2) OVER w_holder_id_by_premium AS next2_end_date,
        SUM(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), product ORDER BY premium ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_sum_2,
        NTH_VALUE(premium, 2) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium
    FROM cleaned_policys
    WINDOW w_holder_id_by_premium AS (PARTITION BY holder_id ORDER BY premium, id), w_all_by_premium AS (ORDER BY premium NULLS LAST, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(premium_avg_6_prior / NULLIF(premium_tercile, 0), 3) AS premium_avg_6_prior_per_premium_tercile
    FROM ranked
    WHERE premium_cume_dist BETWEEN 0.1 AND 0.7
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    end_date,
    premium_rank,
    second_end_date,
    next_start_date,
    premium_tercile,
    premium_dense_rank,
    premium_max_4,
    premium_cume_dist,
    next2_end_date,
    second_premium,
    premium_avg_6_prior_per_premium_tercile,
    DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label,
    ABS(premium_dense_rank - 6) AS premium_dense_rank_distance
FROM tagged
