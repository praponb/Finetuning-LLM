WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE holder_id IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(premium) OVER (PARTITION BY product, status ORDER BY premium DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY YEAR(end_date), status), 0), 4) AS premium_share_of_end_date_year_status,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY YEAR(end_date), status)) AS distinct_holder_id_in_end_date_year_status,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY YEAR(end_date), status)) AS distinct_product_in_end_date_year_status,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(start_date), holder_id ORDER BY end_date, id), 3) AS end_date_percent_rank,
        DATEDIFF(start_date, LAG(start_date) OVER w_status_product_by_end_date) AS days_since_prev_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        ROUND(AVG(premium) OVER (PARTITION BY MONTH(start_date), holder_id ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        NTILE(4) OVER w_product_status_by_start_date AS start_date_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY premium NULLS LAST), 3) AS premium_cume_dist,
        COUNT(*) OVER (PARTITION BY YEAR(end_date), status) AS rows_in_end_date_year_status,
        DATEDIFF(start_date, LAG(start_date) OVER w_status_by_premium) AS days_since_prev_start_date_2,
        MAX(premium) OVER (PARTITION BY product) AS product_max_premium,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product)) AS distinct_holder_id_in_product
    FROM filtered
    WINDOW w_status_product_by_end_date AS (PARTITION BY status, product ORDER BY end_date DESC, id), w_product_status_by_start_date AS (PARTITION BY product, status ORDER BY start_date, id), w_status_by_premium AS (PARTITION BY status ORDER BY premium NULLS LAST, id)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(premium_max_2) OVER (PARTITION BY distinct_product_in_end_date_year_status ORDER BY distinct_product_in_end_date_year_status, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_max_2_sum_2_prior,
        FIRST_VALUE(premium_share_of_product) OVER (PARTITION BY start_date_quartile ORDER BY distinct_holder_id_in_end_date_year_status DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium_share_of_product,
        ROUND(end_date_percent_rank / NULLIF(SUM(end_date_percent_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)), 0), 4) AS end_date_percent_rank_share_of_end_date_month,
        LEAD(premium) OVER (PARTITION BY YEAR(end_date) ORDER BY distinct_product_in_end_date_year_status, id) AS next_premium,
        ROUND(AVG(premium_share_of_product) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)), 2) AS end_date_month_avg_premium_share_of_product,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)) AS rows_in_end_date_month,
        ROUND(AVG(rows_in_end_date_year_status) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)), 2) AS end_date_month_avg_rows_in_end_date_year_status
    FROM sequenced
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    end_date,
    status_slug,
    premium_max_2,
    premium_share_of_end_date_year_status,
    distinct_holder_id_in_end_date_year_status,
    end_date_percent_rank,
    premium_share_of_product,
    premium_avg_6_prior,
    start_date_quartile,
    rows_in_end_date_year_status,
    days_since_prev_start_date_2,
    product_max_premium,
    distinct_holder_id_in_product,
    premium_max_2_sum_2_prior,
    first_premium_share_of_product,
    end_date_percent_rank_share_of_end_date_month,
    rows_in_end_date_month,
    end_date_month_avg_rows_in_end_date_year_status,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown
FROM sequenced_2
WHERE premium_max_2_sum_2_prior <= 40520.0
ORDER BY end_date_percent_rank_share_of_end_date_month DESC
