WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        CASE WHEN product IN ('LIFE', 'TRAVEL') THEN 'group_a' WHEN product = 'AUTO' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE product IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        product,
        start_date,
        status,
        product_group,
        LEAD(premium, 2) OVER w_holder_id_status_by_start_da AS next2_premium,
        premium - LAG(premium) OVER (ORDER BY start_date, id) AS premium_change,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY product, holder_id)) AS distinct_status_in_product_holder_id,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id, status ORDER BY start_date, id), 3) AS start_date_cume_dist,
        ROUND(AVG(premium) OVER (PARTITION BY product, holder_id), 2) AS product_holder_id_avg_premium,
        LAG(start_date) OVER (PARTITION BY holder_id, status ORDER BY start_date, id) AS prev_start_date,
        LAG(premium) OVER (ORDER BY start_date, id) AS prev_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product, holder_id), 0), 4) AS premium_share_of_product_holder_id,
        SUM(premium) OVER (PARTITION BY product, holder_id) AS product_holder_id_sum_premium,
        ROUND(PERCENT_RANK() OVER w_holder_id_status_by_start_da_2, 3) AS start_date_percent_rank,
        LAG(start_date) OVER (PARTITION BY product_group, status ORDER BY start_date, id) AS prev_start_date_2,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product_group, status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date
    FROM recent_policys
    WINDOW w_holder_id_status_by_start_da AS (PARTITION BY holder_id, status ORDER BY start_date, id), w_product_holder_id AS (PARTITION BY product, holder_id), w_holder_id_status_by_start_da_2 AS (PARTITION BY holder_id, status ORDER BY start_date)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        product,
        start_date,
        product_group,
        premium_change,
        distinct_status_in_product_holder_id,
        start_date_cume_dist,
        product_holder_id_avg_premium,
        prev_premium,
        premium_share_of_product_holder_id,
        product_holder_id_sum_premium,
        start_date_percent_rank,
        prev_start_date_2,
        first_known_start_date,
        CONCAT(product_group, '#', CAST(id AS STRING)) AS product_group_ref,
        LEAST(prev_premium, premium_change) AS least_prev_premium_premium_change,
        premium_change + start_date_percent_rank AS premium_change_plus_start_date_percent_rank
    FROM windowed
)
-- Final projection
SELECT
    id,
    product,
    start_date,
    product_group,
    distinct_status_in_product_holder_id,
    start_date_cume_dist,
    prev_premium,
    product_holder_id_sum_premium,
    start_date_percent_rank,
    prev_start_date_2,
    product_group_ref,
    least_prev_premium_premium_change,
    premium_change_plus_start_date_percent_rank
FROM tagged
ORDER BY least_prev_premium_premium_change DESC NULLS FIRST
