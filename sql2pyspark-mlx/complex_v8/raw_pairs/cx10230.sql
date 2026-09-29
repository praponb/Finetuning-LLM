WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DAYOFWEEK(start_date) AS start_date_dayofweek
    FROM policies
    WHERE (end_date < DATE '2026-07-11' OR product NOT IN ('TRAVEL', 'HEALTH'))
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(start_date, 2) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROUND(premium - AVG(premium) OVER (PARTITION BY YEAR(start_date)), 2) AS premium_vs_start_date_year_avg,
        FIRST_VALUE(premium) IGNORE NULLS OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        COUNT(*) OVER (PARTITION BY YEAR(start_date)) AS rows_in_start_date_year,
        SUM(premium) OVER (PARTITION BY MONTH(start_date)) AS start_date_month_sum_premium,
        RANK() OVER (PARTITION BY start_date_dayofweek, product ORDER BY start_date) AS start_date_rank,
        LAG(start_date) OVER (PARTITION BY start_date_dayofweek, product ORDER BY start_date, id) AS prev_start_date,
        SUM(start_date_dayofweek) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_start_date_dayofweek,
        ROW_NUMBER() OVER (ORDER BY start_date DESC, id) AS seq_no,
        LAG(start_date, 3) OVER w_all_by_end_date AS prev3_start_date,
        NTILE(3) OVER w_all_by_end_date AS end_date_tercile,
        ROUND(AVG(premium) OVER (PARTITION BY YEAR(start_date)), 2) AS start_date_year_avg_premium
    FROM cleaned_policys
    WINDOW w_all_by_end_date AS (ORDER BY end_date, id)
),
scored AS (
    -- Label and score rows
    SELECT *, COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM sequenced
    WHERE start_date_dayofweek > 3
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        second_start_date,
        premium_vs_start_date_year_avg,
        first_known_premium,
        start_date_month_sum_premium,
        start_date_rank,
        prev_start_date,
        running_start_date_dayofweek,
        seq_no,
        prev3_start_date,
        end_date_tercile,
        start_date_year_avg_premium,
        product_or_unknown,
        COUNT(*) OVER (PARTITION BY product, start_date_dayofweek ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(first_known_premium) OVER w_product_or_unknown AS product_or_unknown_sum_first_known_premium,
        LAG(first_known_premium) OVER (PARTITION BY status, MONTH(start_date) ORDER BY start_date_year_avg_premium ASC, id) AS prev_first_known_premium,
        MAX(rows_in_start_date_year) OVER (PARTITION BY product_or_unknown) AS product_or_unknown_max_rows_in_start_date_year,
        NTH_VALUE(rows_in_start_date_year, 2) OVER (PARTITION BY status, MONTH(start_date) ORDER BY start_date_year_avg_premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_start_date_year
    FROM scored
    WINDOW w_product_or_unknown AS (PARTITION BY product_or_unknown)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    second_start_date,
    premium_vs_start_date_year_avg,
    first_known_premium,
    start_date_month_sum_premium,
    running_start_date_dayofweek,
    prev3_start_date,
    end_date_tercile,
    product_or_unknown,
    product_or_unknown_sum_first_known_premium,
    prev_first_known_premium,
    product_or_unknown_max_rows_in_start_date_year,
    second_rows_in_start_date_year,
    INITCAP(TRIM(product_or_unknown)) AS product_or_unknown_clean,
    CONCAT_WS('|', product_or_unknown, product) AS product_or_unknown_product_label
FROM windowed
WHERE product_or_unknown = 'HEALTH'
