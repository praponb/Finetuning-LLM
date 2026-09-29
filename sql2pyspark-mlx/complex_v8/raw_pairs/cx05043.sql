WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        ABS(premium - 2319.8) AS premium_distance
    FROM policies
    WHERE holder_id <= 13
      AND status IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        premium,
        end_date,
        status_or_unknown,
        premium_distance,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        NTH_VALUE(premium, 2) OVER (PARTITION BY status_or_unknown ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        LEAD(end_date, 3) OVER (ORDER BY end_date DESC NULLS FIRST, id) AS next3_end_date,
        ROW_NUMBER() OVER (PARTITION BY status_or_unknown ORDER BY end_date DESC, id) AS seq_no,
        ROUND(premium_distance / NULLIF(SUM(premium_distance) OVER (PARTITION BY holder_id), 0), 4) AS premium_distance_share_of_holder_id,
        DENSE_RANK() OVER (ORDER BY end_date DESC NULLS FIRST, id) AS end_date_dense_rank,
        DATEDIFF(end_date, LAG(end_date) OVER (ORDER BY end_date DESC NULLS FIRST, id)) AS days_since_prev_end_date,
        ROUND(AVG(premium_distance) OVER (PARTITION BY holder_id ORDER BY premium ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_distance_avg_6_prior,
        RANK() OVER (ORDER BY end_date DESC NULLS FIRST) AS end_date_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status_or_unknown, product ORDER BY premium_distance, id), 3) AS premium_distance_percent_rank,
        ROUND(AVG(premium) OVER w_product, 2) AS product_avg_premium,
        ROW_NUMBER() OVER w_holder_id_status_or_unknown AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY status_or_unknown ORDER BY end_date DESC), 3) AS end_date_cume_dist,
        ROUND(premium_distance / NULLIF(SUM(premium_distance) OVER (PARTITION BY product), 0), 4) AS premium_distance_share_of_product
    FROM filtered_policys
    WINDOW w_product AS (PARTITION BY product), w_holder_id_status_or_unknown AS (PARTITION BY holder_id, status_or_unknown ORDER BY premium DESC, id)
)
SELECT
    id,
    product,
    end_date,
    status_or_unknown,
    premium_distance,
    second_premium,
    premium_distance_share_of_holder_id,
    end_date_dense_rank,
    days_since_prev_end_date,
    end_date_rank,
    premium_distance_percent_rank,
    product_avg_premium,
    rn,
    premium_distance_share_of_product,
    COALESCE(end_date_dense_rank, 0) AS end_date_dense_rank_filled,
    COALESCE(rn, 0) AS rn_filled
FROM with_windows
ORDER BY second_premium DESC NULLS FIRST
