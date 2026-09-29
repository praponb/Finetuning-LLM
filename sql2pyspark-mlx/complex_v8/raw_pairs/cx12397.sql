WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len,
        ROUND(premium * 1.07, 2) AS premium_adj
    FROM policies
    WHERE holder_id IS NOT NULL
),
per_group AS (
    -- Group-level totals
    SELECT
        status,
        product,
        YEAR(end_date) AS end_date_year,
        ROUND(SUM(status_len) / NULLIF(SUM(premium), 0), 3) AS status_len_to_premium_ratio,
        SUM(premium) AS total_premium,
        PERCENTILE_APPROX(premium, 0.5) AS median_premium,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(premium_adj) / NULLIF(SUM(status_len), 0), 3) AS premium_adj_to_status_len_ratio
    FROM base_policys
    GROUP BY status, product, YEAR(end_date)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(total_premium / NULLIF(distinct_ids, 0), 3) AS total_premium_per_distinct_ids
    FROM per_group
    WHERE total_premium < 16590.0
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY end_date_year, status ORDER BY premium_adj_to_status_len_ratio, status, product, end_date_year), 3) AS premium_adj_to_status_len_ratio_percent_rank,
        MAX(median_premium) OVER (PARTITION BY product, distinct_ids) AS product_distinct_ids_max_median_premium,
        SUM(median_premium) OVER (PARTITION BY product, distinct_ids) AS product_distinct_ids_sum_median_premium,
        end_date_year - LAG(end_date_year) OVER w_end_date_year_status_by_prem AS end_date_year_change,
        NTILE(4) OVER (PARTITION BY end_date_year, status ORDER BY premium_adj_to_status_len_ratio, status, product, end_date_year) AS premium_adj_to_status_len_ratio_quartile,
        LEAD(end_date_year) OVER w_end_date_year_status_by_prem AS next_end_date_year
    FROM scored
    WINDOW w_end_date_year_status_by_prem AS (PARTITION BY end_date_year, status ORDER BY premium_adj_to_status_len_ratio, status, product, end_date_year)
)
SELECT
    status,
    product,
    end_date_year,
    status_len_to_premium_ratio,
    total_premium_per_distinct_ids,
    premium_adj_to_status_len_ratio_percent_rank,
    product_distinct_ids_max_median_premium,
    product_distinct_ids_sum_median_premium,
    premium_adj_to_status_len_ratio_quartile
FROM ranked
ORDER BY end_date_year
