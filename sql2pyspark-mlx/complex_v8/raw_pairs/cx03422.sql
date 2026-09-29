WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        LOWER(TRIM(status)) AS status_clean
    FROM policies
    WHERE end_date < DATE '2026-10-23'
),
with_details_holder AS (
    -- Join policyholders
    SELECT s.*, p.joined_on AS holder_joined_on
    FROM valid_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status_clean,
        holder_joined_on,
        LEAD(end_date) OVER (PARTITION BY product, DATE_TRUNC('MONTH', holder_joined_on) ORDER BY premium, id) AS next_end_date,
        NTILE(10) OVER (PARTITION BY product ORDER BY start_date, id) AS start_date_decile,
        SUM(premium) OVER (PARTITION BY YEAR(start_date)) AS start_date_year_sum_premium,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product, DATE_TRUNC('MONTH', holder_joined_on) ORDER BY premium, id), 3) AS premium_percent_rank,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY holder_id, MONTH(holder_joined_on) ORDER BY holder_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        LAST_VALUE(holder_joined_on) IGNORE NULLS OVER (PARTITION BY holder_id, MONTH(holder_joined_on) ORDER BY holder_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_joined_on,
        LAG(start_date) OVER (PARTITION BY holder_id, MONTH(holder_joined_on) ORDER BY holder_joined_on, id) AS prev_start_date,
        RANK() OVER (PARTITION BY product, DATE_TRUNC('MONTH', holder_joined_on) ORDER BY premium) AS premium_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, MONTH(holder_joined_on) ORDER BY holder_joined_on, id), 3) AS holder_joined_on_percent_rank,
        SIZE(COLLECT_SET(holder_id) OVER w_start_date_year) AS distinct_holder_id_in_start_date_year,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY YEAR(start_date)), 0), 4) AS premium_share_of_start_date_year
    FROM with_details_holder
    WINDOW w_start_date_year AS (PARTITION BY YEAR(start_date))
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status_clean,
        holder_joined_on,
        next_end_date,
        start_date_decile,
        start_date_year_sum_premium,
        premium_percent_rank,
        last_known_start_date,
        last_known_holder_joined_on,
        prev_start_date,
        holder_joined_on_percent_rank,
        distinct_holder_id_in_start_date_year,
        premium_share_of_start_date_year
    FROM sequenced
    WHERE premium_rank = 1
      AND holder_joined_on_percent_rank >= 0.1
)
SELECT
    id,
    product,
    premium,
    start_date,
    end_date,
    status_clean,
    holder_joined_on,
    start_date_decile,
    start_date_year_sum_premium,
    prev_start_date,
    distinct_holder_id_in_start_date_year,
    premium_share_of_start_date_year,
    FLOOR(premium / 1000) * 1000 AS premium_bucket,
    COALESCE(product, 'UNKNOWN') AS product_or_unknown
FROM first_rows
ORDER BY end_date DESC, product DESC NULLS FIRST
