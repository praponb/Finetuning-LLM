WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product = 'AUTO'
      AND holder_id IS NOT NULL
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        p.holder_name,
        p.region AS holder_region
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(start_date) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        ROW_NUMBER() OVER w_holder_region_holder_id_by_e AS rn,
        NTILE(10) OVER (ORDER BY start_date DESC, id) AS start_date_decile,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        LEAD(end_date) OVER (PARTITION BY holder_id, product ORDER BY end_date DESC, id) AS next_end_date,
        NTILE(4) OVER (ORDER BY start_date DESC, id) AS start_date_quartile,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id, product ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, product ORDER BY end_date DESC), 3) AS end_date_percent_rank,
        LEAD(end_date) OVER (ORDER BY start_date DESC, id) AS next_end_date_2,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY holder_region, holder_id ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        NTILE(4) OVER w_all_by_end_date AS end_date_quartile,
        LEAD(end_date, 3) OVER (PARTITION BY holder_id, product ORDER BY end_date DESC, id) AS next3_end_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_region, holder_id ORDER BY end_date DESC, id), 3) AS end_date_percent_rank_2
    FROM with_parent
    WINDOW w_holder_region_holder_id_by_e AS (PARTITION BY holder_region, holder_id ORDER BY end_date DESC, id), w_all_by_end_date AS (ORDER BY end_date, id)
),
first_rows AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE rn <= 3
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(first_known_start_date, 'yyyy-MM') AS first_known_start_date_ym,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        YEAR(last_start_date) AS last_start_date_year,
        ROUND(100 * COUNT(CASE WHEN LENGTH(TRIM(holder_name)) >= 4 THEN 1 END) / COUNT(*), 1) AS pct_length_trim_holder_name_4,
        SUM(CASE WHEN product = 'HEALTH' THEN 1 ELSE 0 END) AS n_product_health,
        DATEDIFF(MAX(next_end_date_2), MIN(next_end_date_2)) AS next_end_date_2_span_days
    FROM first_rows
    GROUP BY 1, 2, 3
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(last_start_date_year) OVER (PARTITION BY n_product_health, last_start_date_year ORDER BY next_end_date_2_span_days DESC, first_known_start_date_ym, end_date_ym, last_start_date_year ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS last_start_date_year_avg_1_prior,
        MAX(next_end_date_2_span_days) OVER (PARTITION BY n_product_health, last_start_date_year ORDER BY next_end_date_2_span_days DESC, first_known_start_date_ym, end_date_ym, last_start_date_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_next_end_date_2_span_days_so_far
    FROM totals
)
-- Final projection
SELECT
    first_known_start_date_ym,
    end_date_ym,
    last_start_date_year,
    n_product_health,
    next_end_date_2_span_days,
    last_start_date_year_avg_1_prior,
    max_next_end_date_2_span_days_so_far,
    INITCAP(TRIM(end_date_ym)) AS end_date_ym_clean,
    LEAST(n_product_health, last_start_date_year) AS least_n_product_health_last_start_date_year
FROM sequenced
ORDER BY end_date_ym
