WITH base AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        LOWER(segment) AS segment_lower,
        LAST_DAY(signup_date) AS signup_date_month_end
    FROM customers
    WHERE signup_date >= DATE '2023-07-30'
      AND age != 65
),
enriched AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY credit_limit DESC, id ASC) AS credit_limit_bucket,
        COUNT(credit_limit) OVER (PARTITION BY city, segment) AS city_count_credit_limit
    FROM base
),
latest AS (
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY country, city ORDER BY credit_limit DESC) = 1
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id ASC) AS dedup_rank
    FROM latest
    QUALIFY dedup_rank = 1
),
enriched_customers AS (
    SELECT
        id,
        email,
        credit_limit,
        segment_lower,
        credit_limit_bucket,
        dedup_rank,
        COUNT(city_count_credit_limit) OVER (PARTITION BY city, segment ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_city_count_credit_limit,
        SUM(city_count_credit_limit) OVER (PARTITION BY city, segment ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_city_count_credit_limit,
        AVG(city_count_credit_limit) OVER (PARTITION BY city, segment ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_city_count_credit_limit
    FROM ranked
)
SELECT
    id,
    email,
    dedup_rank,
    running_sum_city_count_credit_limit,
    running_avg_city_count_credit_limit
FROM enriched_customers
ORDER BY running_avg_city_count_credit_limit DESC NULLS FIRST
