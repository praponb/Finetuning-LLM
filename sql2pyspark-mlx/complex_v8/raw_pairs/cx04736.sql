WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix,
        YEAR(end_date) AS end_date_year
    FROM policies
    WHERE product IS NOT NULL
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT s.*, p.region AS holder_region, p.holder_name
    FROM filtered s
    JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        end_date,
        status,
        product_prefix,
        end_date_year,
        holder_region,
        holder_name,
        ROUND(AVG(end_date_year) OVER (PARTITION BY holder_region, end_date_year), 2) AS holder_region_end_date_year_avg_end_date_year,
        FIRST_VALUE(end_date_year) IGNORE NULLS OVER (ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date_year,
        SIZE(COLLECT_SET(status) OVER w_holder_region_end_date_year) AS distinct_status_in_holder_region_end_date_year,
        SUM(end_date_year) OVER (PARTITION BY holder_region, end_date_year ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS end_date_year_sum_7,
        RANK() OVER (PARTITION BY holder_region, end_date_year ORDER BY end_date) AS end_date_rank,
        NTILE(4) OVER (PARTITION BY holder_region, end_date_year ORDER BY end_date, id) AS end_date_quartile,
        ROUND(end_date_year - AVG(end_date_year) OVER (PARTITION BY holder_region, end_date_year), 2) AS end_date_year_vs_holder_region_end_date_year_avg,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY holder_region, end_date_year)) AS distinct_holder_id_in_holder_region_end_date_yea,
        ROUND(AVG(end_date_year) OVER (ORDER BY premium DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS end_date_year_avg_4
    FROM with_parent_holder
    WINDOW w_holder_region_end_date_year AS (PARTITION BY holder_region, end_date_year)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE end_date_rank <= 5
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        end_date,
        holder_region,
        holder_name,
        first_known_end_date_year,
        distinct_status_in_holder_region_end_date_year,
        end_date_year_sum_7,
        end_date_quartile,
        end_date_year_vs_holder_region_end_date_year_avg,
        end_date_year_avg_4,
        SUM(end_date_year) OVER (PARTITION BY distinct_holder_id_in_holder_region_end_date_yea ORDER BY first_known_end_date_year DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_end_date_year,
        NTH_VALUE(end_date_year_avg_4, 2) OVER (PARTITION BY distinct_status_in_holder_region_end_date_year ORDER BY end_date_year_sum_7 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date_year_avg_4,
        RANK() OVER w_distinct_status_in_holder_re_2 AS end_date_year_sum_7_rank,
        MAX(end_date_quartile) OVER (PARTITION BY end_date_rank ORDER BY holder_region_end_date_year_avg_end_date_year, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS end_date_quartile_max_7,
        MAX(end_date_year) OVER w_end_date_month_holder_region AS end_date_month_holder_region_max_end_date_year,
        LAG(end_date_year_sum_7, 3) OVER w_holder_region_distinct_holde AS prev3_end_date_year_sum_7,
        MAX(end_date_year) OVER (PARTITION BY distinct_status_in_holder_region_end_date_year ORDER BY end_date_year_sum_7 ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS end_date_year_max_7
    FROM first_rows
    WINDOW w_distinct_status_in_holder_re_2 AS (PARTITION BY distinct_status_in_holder_region_end_date_year ORDER BY end_date_year_sum_7 ASC), w_end_date_month_holder_region AS (PARTITION BY MONTH(end_date), holder_region), w_holder_region_distinct_holde AS (PARTITION BY holder_region, distinct_holder_id_in_holder_region_end_date_yea ORDER BY end_date_year_sum_7 DESC, id)
)
-- Final projection
SELECT
    id,
    holder_id,
    holder_region,
    holder_name,
    distinct_status_in_holder_region_end_date_year,
    end_date_year_sum_7,
    end_date_quartile,
    running_end_date_year,
    second_end_date_year_avg_4,
    end_date_year_sum_7_rank,
    end_date_quartile_max_7,
    end_date_month_holder_region_max_end_date_year,
    end_date_year_max_7
FROM windowed
WHERE end_date_quartile <= 9
ORDER BY holder_id DESC, id
LIMIT 20
