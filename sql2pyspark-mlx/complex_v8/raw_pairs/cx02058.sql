WITH valid_listeners AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        country,
        birth_year,
        interests,
        DATE_FORMAT(signup_date, 'yyyy-MM-dd') AS signup_date_label,
        ABS(birth_year - 1970) AS birth_year_distance
    FROM listeners
    WHERE ARRAY_CONTAINS(interests, 'podcasts')
),
unique_rows AS (
    -- Keep the earliest row per country
    SELECT
        id,
        username,
        plan,
        country,
        birth_year,
        interests,
        signup_date_label,
        birth_year_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY birth_year_distance, id) AS rn
        FROM valid_listeners
    ) ranked
    WHERE rn = 1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        country,
        birth_year,
        interests,
        signup_date_label,
        birth_year_distance,
        ROUND(AVG(birth_year) OVER (PARTITION BY plan, country ORDER BY birth_year NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS birth_year_avg_2_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY plan, country ORDER BY birth_year NULLS LAST), 3) AS birth_year_cume_dist,
        MAX(birth_year_distance) OVER w_plan AS plan_max_birth_year_distance,
        SIZE(COLLECT_SET(country) OVER (PARTITION BY plan)) AS distinct_country_in_plan,
        RANK() OVER w_plan_by_birth_year_ties AS birth_year_rank
    FROM unique_rows
    WINDOW w_plan AS (PARTITION BY plan), w_plan_by_birth_year_ties AS (PARTITION BY plan ORDER BY birth_year)
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE birth_year_rank < (SELECT MAX(birth_year_rank) FROM sequenced)
)
SELECT id, field, reading
FROM (
    SELECT id, birth_year_avg_2_prior, birth_year_cume_dist
    FROM filtered
) src
UNPIVOT (
    reading FOR field IN (birth_year_avg_2_prior, birth_year_cume_dist)
) unp
ORDER BY id, field
