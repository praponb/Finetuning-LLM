WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        LEAST(credit_limit, age) AS least_credit_limit_age,
        UPPER(email) AS email_upper,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM customers
    WHERE is_active
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        country,
        city,
        signup_date,
        email_upper,
        LEAD(least_credit_limit_age, 2) OVER (PARTITION BY country ORDER BY signup_date, id) AS next_least_credit_limit_age,
        FIRST_VALUE(least_credit_limit_age) IGNORE NULLS OVER w_grp AS first_known_least_credit_limit_age,
        DENSE_RANK() OVER w_part AS credit_limit_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY least_credit_limit_age DESC, id) AS row_seq
    FROM filtered
    WHERE MONTH(signup_date) IN (1, 8, 12)
    WINDOW w_grp AS (ORDER BY least_credit_limit_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY country, city ORDER BY credit_limit NULLS FIRST)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        name,
        country,
        signup_date,
        email_upper,
        next_least_credit_limit_age,
        credit_limit_dense_rank,
        row_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit_dense_rank DESC NULLS FIRST, id) AS rn
        FROM with_windows
    ) x
    WHERE rn = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY signup_date) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        signup_date,
        email_upper,
        next_least_credit_limit_age,
        credit_limit_dense_rank,
        row_seq,
        CASE WHEN credit_limit_dense_rank >= 7 THEN 'High' WHEN credit_limit_dense_rank >= 3 THEN 'Medium' ELSE 'Low' END AS credit_limit_dense_rank_level,
        ROUND(dedup_rank, 0) AS dedup_rank_rounded
    FROM latest
    WHERE LENGTH(name) < 5
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.start_date < s.signup_date
        )
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        signup_date,
        email_upper,
        next_least_credit_limit_age,
        credit_limit_dense_rank,
        row_seq,
        credit_limit_dense_rank_level,
        dedup_rank_rounded,
        CAST(row_seq AS STRING) AS row_seq_num
    FROM eligible
    WHERE signup_date > DATE '2023-08-24'
),
report AS (
    SELECT
        id,
        name,
        country,
        signup_date,
        email_upper,
        next_least_credit_limit_age,
        credit_limit_dense_rank,
        row_seq,
        credit_limit_dense_rank_level
    FROM base
)
SELECT
    id,
    country,
    signup_date,
    email_upper,
    next_least_credit_limit_age,
    row_seq,
    CASE WHEN country RLIKE '^(JP|SG)$' THEN 'PRIORITY' WHEN country RLIKE '^(TH|US)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS country_zone
FROM report;
