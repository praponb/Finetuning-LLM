WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE premium <= 4678.8
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(premium) OVER (PARTITION BY YEAR(end_date), product ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id), 0), 4) AS premium_share_of_holder_id,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product ORDER BY start_date DESC), 3) AS start_date_cume_dist,
        LEAD(end_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product ORDER BY start_date DESC, id) AS next2_end_date,
        SUM(premium) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_2,
        MAX(premium) OVER (PARTITION BY holder_id) AS holder_id_max_premium,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_2,
        SUM(premium) OVER (PARTITION BY YEAR(end_date), product ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_sum_3_prior,
        COUNT(*) OVER w_start_date_year_holder_id AS rows_in_start_date_year_holder_id,
        ROUND(AVG(premium) OVER (ORDER BY start_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY YEAR(start_date), holder_id), 0), 4) AS premium_share_of_start_date_year_holder_id,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium
    FROM valid_policys
    WINDOW w_start_date_year_holder_id AS (PARTITION BY YEAR(start_date), holder_id)
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        YEAR(next2_end_date) AS next2_end_date_year,
        DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days,
        ROUND(STDDEV(premium_share_of_start_date_year_holder_id), 2) AS stddev_premium_share_of_start_date_year_holder_i,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list
    FROM with_trends
    WHERE premium_share_of_holder_id >= 0.1
    GROUP BY DATE_FORMAT(start_date, 'yyyy-MM'), YEAR(next2_end_date)
    HAVING AVG(premium_avg_2) > 1160.0
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY stddev_premium_share_of_start_date_year_holder_i, start_date_ym, next2_end_date_year), 3) AS stddev_premium_share_of_start_date_year_holder_i_2,
        next2_end_date_year - LAG(next2_end_date_year) OVER (ORDER BY stddev_premium_share_of_start_date_year_holder_i, start_date_ym, next2_end_date_year) AS next2_end_date_year_change
    FROM summary
)
-- Final projection
SELECT
    start_date_ym,
    next2_end_date_year,
    end_date_span_days,
    stddev_premium_share_of_start_date_year_holder_i,
    product_list,
    next2_end_date_year_change,
    SUBSTRING(TRIM(start_date_ym), 1, 4) AS start_date_ym_prefix
FROM sequenced
ORDER BY next2_end_date_year DESC
