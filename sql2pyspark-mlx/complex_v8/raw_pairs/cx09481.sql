WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE end_date < DATE '2026-12-05'
      AND premium < 3465.4
),
with_details AS (
    -- Join policyholders
    SELECT s.*, p.joined_on AS holder_joined_on
    FROM filtered_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        holder_joined_on,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, product), 0), 4) AS premium_share_of_holder_id_product,
        LAG(end_date) OVER (PARTITION BY status ORDER BY holder_joined_on DESC, id) AS prev_end_date,
        COUNT(*) OVER (PARTITION BY status ORDER BY holder_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(holder_joined_on) OVER (PARTITION BY status ORDER BY holder_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_holder_joined_on,
        FIRST_VALUE(holder_joined_on) IGNORE NULLS OVER (PARTITION BY YEAR(start_date) ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_holder_joined_on,
        ROW_NUMBER() OVER w_start_date_year_by_premium AS rn,
        DENSE_RANK() OVER w_status_by_holder_joined_on AS holder_joined_on_dense_rank,
        SUM(premium) OVER (PARTITION BY status ORDER BY holder_joined_on DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_sum_7,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, product), 2) AS holder_id_product_avg_premium,
        MAX(premium) OVER (PARTITION BY holder_id) AS holder_id_max_premium,
        ROUND(premium - AVG(premium) OVER w_holder_id_product, 2) AS premium_vs_holder_id_product_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(start_date) ORDER BY premium DESC), 3) AS premium_percent_rank,
        DENSE_RANK() OVER w_product_by_end_date_ties AS end_date_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY product ORDER BY end_date, id), 3) AS end_date_cume_dist,
        FIRST_VALUE(end_date) OVER (PARTITION BY YEAR(start_date) ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date
    FROM with_details
    WINDOW w_start_date_year_by_premium AS (PARTITION BY YEAR(start_date) ORDER BY premium DESC, id), w_status_by_holder_joined_on AS (PARTITION BY status ORDER BY holder_joined_on DESC, id), w_holder_id_product AS (PARTITION BY holder_id, product), w_product_by_end_date_ties AS (PARTITION BY product ORDER BY end_date)
)
SELECT
    id,
    holder_id,
    premium,
    end_date,
    status,
    premium_share_of_holder_id_product,
    first_known_holder_joined_on,
    rn,
    holder_joined_on_dense_rank,
    premium_sum_7,
    holder_id_product_avg_premium,
    holder_id_max_premium,
    premium_vs_holder_id_product_avg,
    end_date_cume_dist,
    first_end_date,
    DATE_FORMAT(last_holder_joined_on, 'yyyy-MM-dd') AS last_holder_joined_on_label
FROM windowed
ORDER BY rn, premium DESC NULLS FIRST, id
LIMIT 5
