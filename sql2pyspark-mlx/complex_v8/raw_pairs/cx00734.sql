WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE YEAR(start_date) = 2024
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        start_date,
        end_date,
        LAG(start_date) OVER w_holder_id_end_date_month_by AS prev_start_date,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY MONTH(start_date), product ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        DENSE_RANK() OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date) ORDER BY start_date NULLS LAST, id) AS start_date_dense_rank,
        LAST_VALUE(end_date) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date) ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), holder_id ORDER BY end_date, id) AS end_date_quintile,
        RANK() OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date) ORDER BY start_date NULLS LAST) AS start_date_rank,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY MONTH(start_date), product ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY MONTH(start_date), product ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date) ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        ROUND(PERCENT_RANK() OVER w_all_by_start_date, 3) AS start_date_percent_rank,
        LAG(end_date) OVER (PARTITION BY MONTH(start_date), product ORDER BY start_date NULLS LAST, id) AS prev_end_date
    FROM cleaned_policys
    WINDOW w_holder_id_end_date_month_by AS (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date) ORDER BY start_date NULLS LAST, id), w_all_by_start_date AS (ORDER BY start_date DESC NULLS FIRST, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(prev_start_date, 'yyyy-MM') AS prev_start_date_ym,
        end_date_quintile,
        COUNT(*) AS row_count,
        SUM(CASE WHEN product NOT IN ('TRAVEL', 'HOME') THEN 1 ELSE 0 END) AS n_product_not_in_travel_home,
        DATEDIFF(MAX(second_end_date), MIN(second_end_date)) AS second_end_date_span_days,
        ROUND(STDDEV(start_date_rank), 2) AS stddev_start_date_rank
    FROM ranked
    GROUP BY DATE_FORMAT(prev_start_date, 'yyyy-MM'), end_date_quintile
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        prev_start_date_ym,
        end_date_quintile,
        second_end_date_span_days,
        stddev_start_date_rank,
        COUNT(*) OVER (PARTITION BY end_date_quintile) AS rows_in_end_date_quintile,
        ROUND(n_product_not_in_travel_home / NULLIF(SUM(n_product_not_in_travel_home) OVER w_end_date_quintile, 0), 4) AS n_product_not_in_travel_home_share_of_end_date_q,
        NTH_VALUE(n_product_not_in_travel_home, 2) OVER (ORDER BY n_product_not_in_travel_home DESC, prev_start_date_ym, end_date_quintile ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_product_not_in_travel_home,
        DENSE_RANK() OVER (ORDER BY n_product_not_in_travel_home DESC) AS n_product_not_in_travel_home_dense_rank
    FROM grouped
    WINDOW w_end_date_quintile AS (PARTITION BY end_date_quintile)
)
-- Final projection
SELECT
    prev_start_date_ym,
    end_date_quintile,
    second_end_date_span_days,
    rows_in_end_date_quintile,
    n_product_not_in_travel_home_share_of_end_date_q,
    n_product_not_in_travel_home_dense_rank,
    end_date_quintile + stddev_start_date_rank AS end_date_quintile_plus_stddev_start_date_rank,
    LENGTH(TRIM(prev_start_date_ym)) AS prev_start_date_ym_len
FROM sequenced
ORDER BY prev_start_date_ym_len, n_product_not_in_travel_home_dense_rank DESC
