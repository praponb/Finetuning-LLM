WITH prepared AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        interests
    FROM listeners
    WHERE SIZE(interests) >= 2
      AND plan IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        interests,
        ROUND(PERCENT_RANK() OVER w_plan_country_by_signup_date, 3) AS signup_date_percent_rank,
        LEAD(signup_date) OVER (PARTITION BY plan, country ORDER BY signup_date DESC, id) AS next_signup_date,
        DENSE_RANK() OVER (PARTITION BY MONTH(signup_date), country ORDER BY signup_date NULLS LAST) AS signup_date_dense_rank,
        NTH_VALUE(signup_date, 2) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date,
        NTH_VALUE(signup_date, 2) OVER (PARTITION BY plan, country ORDER BY signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date_2
    FROM prepared
    WINDOW w_plan_country_by_signup_date AS (PARTITION BY plan, country ORDER BY signup_date DESC)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(signup_date_dense_rank - 6) AS signup_date_dense_rank_distance,
        CONCAT(username, '#', CAST(id AS STRING)) AS username_ref,
        DATE_TRUNC('QUARTER', signup_date) AS signup_date_quarter
    FROM windowed
    WHERE UPPER(TRIM(username)) LIKE 'G%'
),
latest_per_key AS (
    -- Keep the earliest row per country
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        interests,
        signup_date_percent_rank,
        next_signup_date,
        signup_date_dense_rank,
        second_signup_date,
        second_signup_date_2,
        signup_date_dense_rank_distance,
        username_ref,
        signup_date_quarter
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY second_signup_date_2, id) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
)
SELECT id, field, reading
FROM (
    SELECT id, signup_date_dense_rank_distance, signup_date_dense_rank
    FROM latest_per_key
) src
UNPIVOT (
    reading FOR field IN (signup_date_dense_rank_distance, signup_date_dense_rank)
) unp
