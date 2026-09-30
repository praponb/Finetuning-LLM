WITH base_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        DATE_TRUNC('QUARTER', signup_date) AS signup_date_quarter,
        ABS(birth_year - 1970) AS birth_year_distance,
        CASE WHEN plan IN ('family', 'free') THEN 'group_a' WHEN plan = 'student' THEN 'group_b' ELSE 'other' END AS plan_group
    FROM listeners
    WHERE YEAR(signup_date) = 2025
      AND UPPER(TRIM(username)) LIKE 'C%'
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(birth_year, 0) AS birth_year_filled,
        CONCAT_WS(' / ', plan, username) AS plan_username_label
    FROM base_listeners
    WHERE birth_year BETWEEN 1966 AND 1981
),
exploded AS (
    -- One row per interest
    SELECT
        s.id,
        s.username,
        s.plan,
        s.country,
        s.signup_date,
        s.birth_year,
        s.signup_date_quarter,
        s.birth_year_distance,
        s.plan_group,
        s.birth_year_filled,
        s.plan_username_label,
        t.interest_pos,
        t.interest
    FROM enriched s
    LATERAL VIEW POSEXPLODE(s.interests) t AS interest_pos, interest
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM exploded
    WHERE interest_pos > (SELECT AVG(interest_pos) FROM exploded)
)
SELECT
    id,
    username,
    plan,
    country,
    signup_date,
    birth_year,
    signup_date_quarter,
    birth_year_distance,
    plan_group,
    interest_pos,
    interest,
    ABS(birth_year - 1975) AS birth_year_distance_2
FROM screened
ORDER BY id, country
