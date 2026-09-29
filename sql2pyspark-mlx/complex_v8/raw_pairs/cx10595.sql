WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM policies
    WHERE end_date BETWEEN DATE '2025-12-05' AND DATE '2027-05-16'
),
enriched_holder AS (
    -- Join policyholders
    SELECT
        s.*,
        p.joined_on AS holder_joined_on,
        p.holder_name,
        p.region AS holder_region
    FROM base s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(premium) OVER (PARTITION BY status, holder_id ORDER BY end_date NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        ROUND(days_end_date_to_start_date / NULLIF(SUM(days_end_date_to_start_date) OVER (PARTITION BY YEAR(holder_joined_on)), 0), 4) AS days_end_date_to_start_date_share_of_holder_join,
        SUM(days_end_date_to_start_date) OVER (PARTITION BY holder_region, product ORDER BY holder_joined_on DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_end_date_to_start_date_sum_1_prior,
        COUNT(*) OVER w_holder_joined_on_year AS rows_in_holder_joined_on_year,
        ROUND(PERCENT_RANK() OVER (ORDER BY days_end_date_to_start_date DESC NULLS FIRST, id), 3) AS days_end_date_to_start_date_percent_rank,
        ROW_NUMBER() OVER w_all_by_days_end_date_to_star AS days_end_date_to_start_date_seq,
        SIZE(COLLECT_SET(holder_region) OVER w_holder_joined_on_year) AS distinct_holder_region_in_holder_joined_on_year,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY YEAR(holder_joined_on)), 0), 4) AS premium_share_of_holder_joined_on_year,
        ROUND(AVG(days_end_date_to_start_date) OVER (PARTITION BY YEAR(holder_joined_on)), 2) AS holder_joined_on_year_avg_days_end_date_to_start,
        RANK() OVER w_status_holder_id_by_end_date_2 AS end_date_rank
    FROM enriched_holder
    WINDOW w_holder_joined_on_year AS (PARTITION BY YEAR(holder_joined_on)), w_all_by_days_end_date_to_star AS (ORDER BY days_end_date_to_start_date DESC NULLS FIRST, id), w_status_holder_id_by_end_date_2 AS (PARTITION BY status, holder_id ORDER BY end_date NULLS LAST)
),
top_rows AS (
    -- Top 3 per group
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        days_end_date_to_start_date,
        holder_joined_on,
        holder_name,
        holder_region,
        premium_avg_6_prior,
        days_end_date_to_start_date_share_of_holder_join,
        days_end_date_to_start_date_sum_1_prior,
        rows_in_holder_joined_on_year,
        days_end_date_to_start_date_percent_rank,
        distinct_holder_region_in_holder_joined_on_year,
        premium_share_of_holder_joined_on_year,
        holder_joined_on_year_avg_days_end_date_to_start,
        end_date_rank
    FROM windowed
    WHERE days_end_date_to_start_date_seq <= 3
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(premium) OVER w_all_by_days_end_date_to_star_2 AS prev_premium,
        SIZE(COLLECT_SET(holder_region) OVER (PARTITION BY distinct_holder_region_in_holder_joined_on_year, end_date_rank)) AS distinct_holder_region_in_distinct_holder_region,
        ROUND(days_end_date_to_start_date / NULLIF(SUM(days_end_date_to_start_date) OVER (PARTITION BY distinct_holder_region_in_holder_joined_on_year, end_date_rank), 0), 4) AS days_end_date_to_start_date_share_of_distinct_ho,
        NTILE(10) OVER (ORDER BY days_end_date_to_start_date, id) AS days_end_date_to_start_date_decile,
        LAG(premium_avg_6_prior) OVER w_all_by_days_end_date_to_star_2 AS prev_premium_avg_6_prior,
        COUNT(*) OVER (PARTITION BY distinct_holder_region_in_holder_joined_on_year, end_date_rank) AS rows_in_distinct_holder_region_in_holder_joined_
    FROM top_rows
    WINDOW w_all_by_days_end_date_to_star_2 AS (ORDER BY days_end_date_to_start_date_percent_rank DESC, id)
),
totals AS (
    -- Group-level totals
    SELECT
        distinct_holder_region_in_holder_joined_on_year,
        DATE_FORMAT(holder_joined_on, 'yyyy-MM') AS holder_joined_on_ym,
        ROUND(100 * COUNT(CASE WHEN holder_region = 'North' THEN 1 END) / COUNT(*), 1) AS pct_holder_region_north,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE product IS NOT NULL) AS n_product_is_not_null,
        PERCENTILE_APPROX(holder_joined_on_year_avg_days_end_date_to_start, 0.5) AS median_holder_joined_on_year_avg_days_end_date_t,
        DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days,
        PERCENTILE_APPROX(rows_in_distinct_holder_region_in_holder_joined_, 0.5) AS median_rows_in_distinct_holder_region_in_holder_
    FROM windowed_2
    GROUP BY distinct_holder_region_in_holder_joined_on_year, DATE_FORMAT(holder_joined_on, 'yyyy-MM')
    HAVING MAX(distinct_holder_region_in_holder_joined_on_year) > 5
)
SELECT
    distinct_holder_region_in_holder_joined_on_year,
    holder_joined_on_ym,
    pct_holder_region_north,
    cnt,
    median_holder_joined_on_year_avg_days_end_date_t,
    end_date_span_days,
    median_rows_in_distinct_holder_region_in_holder_,
    ROUND(pct_holder_region_north * 0.25, 2) AS pct_holder_region_north_adj,
    LEAST(cnt, n_product_is_not_null) AS least_cnt_n_product_is_not_null
FROM totals
ORDER BY distinct_holder_region_in_holder_joined_on_year DESC
