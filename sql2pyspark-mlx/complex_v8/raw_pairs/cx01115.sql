WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM policies
    WHERE premium BETWEEN 1541.8 AND 3544.1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        product_prefix,
        ROUND(AVG(premium) OVER (PARTITION BY status), 2) AS status_avg_premium,
        ROW_NUMBER() OVER (PARTITION BY product, status ORDER BY start_date, id) AS seq_no,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product), 2) AS premium_vs_product_avg,
        MIN(premium) OVER w_product AS product_min_premium,
        ROUND(CUME_DIST() OVER w_all_by_start_date_ties, 3) AS start_date_cume_dist,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        RANK() OVER w_product_status_by_start_date_2 AS start_date_rank
    FROM cleaned
    WINDOW w_product AS (PARTITION BY product), w_all_by_start_date_ties AS (ORDER BY start_date DESC), w_product_status_by_start_date_2 AS (PARTITION BY product, status ORDER BY start_date)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY start_date_rank, holder_id)) AS distinct_product_in_start_date_rank_holder_id,
        LAG(premium_share_of_product) OVER (ORDER BY premium ASC, id) AS prev_premium_share_of_product,
        ROUND(premium_vs_product_avg / NULLIF(SUM(premium_vs_product_avg) OVER (PARTITION BY start_date_rank, holder_id), 0), 4) AS premium_vs_product_avg_share_of_start_date_rank_,
        MIN(product_min_premium) OVER (PARTITION BY start_date_rank, holder_id) AS start_date_rank_holder_id_min_product_min_premiu,
        LEAD(premium_vs_product_avg, 3) OVER w_all_by_premium AS next3_premium_vs_product_avg,
        premium - LAG(premium) OVER w_all_by_premium AS premium_change,
        NTILE(4) OVER (ORDER BY product_min_premium NULLS LAST, id) AS product_min_premium_quartile
    FROM sequenced
    WINDOW w_all_by_premium AS (ORDER BY premium ASC, id)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    status,
    product_prefix,
    status_avg_premium,
    seq_no,
    premium_vs_product_avg,
    product_min_premium,
    start_date_cume_dist,
    premium_share_of_product,
    start_date_rank,
    prev_premium_share_of_product,
    premium_vs_product_avg_share_of_start_date_rank_,
    start_date_rank_holder_id_min_product_min_premiu
FROM with_trends
ORDER BY premium_share_of_product, id DESC
LIMIT 20
