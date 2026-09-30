WITH valid_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        COALESCE(birth_year, 0) AS birth_year_filled,
        ABS(birth_year - 1980) AS birth_year_distance,
        SUBSTRING(TRIM(plan), 1, 3) AS plan_prefix
    FROM listeners
    WHERE SIZE(interests) >= 1
      AND birth_year BETWEEN 1968 AND 1981
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        username,
        birth_year,
        interests,
        birth_year_filled,
        birth_year_distance,
        plan_prefix,
        ABS(birth_year_filled - 1974) AS birth_year_filled_distance,
        CONCAT(username, '#', CAST(birth_year_distance AS STRING)) AS username_ref,
        QUARTER(signup_date) AS signup_date_quarter
    FROM valid_listeners
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER w_signup_date_quarter_by_birth AS birth_year_filled_decile,
        MAX(birth_year_filled_distance) OVER (PARTITION BY signup_date_quarter ORDER BY birth_year_filled, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS birth_year_filled_distance_max_2_prior
    FROM derived
    WINDOW w_signup_date_quarter_by_birth AS (PARTITION BY signup_date_quarter ORDER BY birth_year_filled, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        birth_year,
        interests,
        birth_year_filled,
        plan_prefix,
        birth_year_filled_distance,
        username_ref,
        birth_year_filled_distance_max_2_prior,
        COALESCE(birth_year_filled, 0) AS birth_year_filled_filled
    FROM windowed
),
windowed_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        birth_year,
        interests,
        birth_year_filled,
        plan_prefix,
        birth_year_filled_distance,
        username_ref,
        birth_year_filled_distance_max_2_prior,
        NTH_VALUE(birth_year, 2) OVER (ORDER BY birth_year_filled_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_birth_year,
        NTILE(3) OVER w_all_by_birth_year AS birth_year_tercile,
        LAG(birth_year_filled_distance, 2) OVER (ORDER BY birth_year ASC, id) AS prev2_birth_year_filled_distance,
        SUM(birth_year_filled_distance) OVER (ORDER BY birth_year ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS birth_year_filled_distance_sum_2_prior,
        birth_year_filled_distance - LAG(birth_year_filled_distance) OVER (ORDER BY birth_year ASC, id) AS birth_year_filled_distance_change,
        ROUND(AVG(birth_year_filled) OVER (ORDER BY birth_year ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS birth_year_filled_avg_4
    FROM annotated
    WINDOW w_all_by_birth_year AS (ORDER BY birth_year ASC, id)
)
SELECT
    id,
    birth_year,
    interests,
    birth_year_filled,
    plan_prefix,
    birth_year_filled_distance_max_2_prior,
    second_birth_year,
    birth_year_tercile,
    prev2_birth_year_filled_distance,
    birth_year_filled_distance_change,
    LEAST(birth_year_tercile, birth_year) AS least_birth_year_tercile_birth_year
FROM windowed_2
WHERE birth_year_filled > 1970
ORDER BY birth_year_filled_distance_change
