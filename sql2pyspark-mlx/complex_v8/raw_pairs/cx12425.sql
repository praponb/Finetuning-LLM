WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        CASE WHEN product IN ('HOME', 'TRAVEL') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE start_date BETWEEN DATE '2024-03-15' AND DATE '2025-05-30'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(premium_bucket) OVER w_holder_id_product_group AS holder_id_product_group_sum_premium_bucket,
        DENSE_RANK() OVER (PARTITION BY product, holder_id ORDER BY premium ASC NULLS LAST, id) AS premium_dense_rank,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY product, status ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product, holder_id ORDER BY premium ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        ROUND(AVG(premium_bucket) OVER (PARTITION BY status), 2) AS status_avg_premium_bucket,
        ROUND(premium_bucket - AVG(premium_bucket) OVER w_status, 2) AS premium_bucket_vs_status_avg,
        premium - LAG(premium) OVER (ORDER BY premium ASC, id) AS premium_change,
        ROW_NUMBER() OVER (PARTITION BY product, status ORDER BY premium, id) AS premium_seq,
        ROUND(premium_bucket / NULLIF(SUM(premium_bucket) OVER w_status, 0), 4) AS premium_bucket_share_of_status,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product, holder_id ORDER BY premium ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        NTILE(4) OVER w_status_by_premium_bucket AS premium_bucket_quartile,
        DENSE_RANK() OVER (ORDER BY premium ASC, id) AS premium_dense_rank_2,
        FIRST_VALUE(premium_bucket) OVER (ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium_bucket,
        COUNT(*) OVER w_status AS rows_in_status
    FROM valid
    WINDOW w_holder_id_product_group AS (PARTITION BY holder_id, product_group), w_status AS (PARTITION BY status), w_status_by_premium_bucket AS (PARTITION BY status ORDER BY premium_bucket ASC, id)
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    premium_bucket,
    product_group,
    holder_id_product_group_sum_premium_bucket,
    premium_dense_rank,
    second_start_date,
    last_known_start_date,
    premium_bucket_vs_status_avg,
    premium_change,
    premium_bucket_quartile,
    premium_dense_rank_2,
    first_premium_bucket,
    ROUND(premium_change * 0.25, 2) AS premium_change_adj,
    ROUND(premium_change * 1.07, 2) AS premium_change_adj_2
FROM ranked
ORDER BY premium_change_adj_2, second_start_date DESC NULLS FIRST
