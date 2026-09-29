WITH base AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        SUBSTRING(email, 1, 3) AS email_prefix
    FROM customers
    WHERE signup_date >= DATE '2023-03-17'
      AND city IS NOT NULL
),
deduped AS (
    SELECT *
    FROM base
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY signup_date) = 1
),
enriched AS (
    SELECT
        *,
        AVG(credit_limit) OVER (PARTITION BY segment) AS segment_avg_credit_limit,
        MIN(credit_limit) OVER w_hist AS city_min_credit_limit,
        FIRST_VALUE(segment, TRUE) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_segment
    FROM deduped
    WINDOW w_hist AS (PARTITION BY city)
),
with_windows AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_percent_rank,
        NTILE(4) OVER (ORDER BY age, id) AS age_bucket
    FROM enriched
)
SELECT
    id,
    name,
    city,
    segment,
    is_active,
    credit_limit,
    email_prefix,
    segment_avg_credit_limit,
    city_min_credit_limit,
    CASE WHEN name RLIKE '^(  Carol Diaz|Mia Rossi)$' THEN 'CORE' ELSE 'EXTENDED' END AS name_zone
FROM with_windows
ORDER BY city_min_credit_limit DESC NULLS LAST
