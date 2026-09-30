WITH cleaned_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        DATE_ADD(signup_date, 30) AS signup_date_plus_30d
    FROM listeners
    WHERE username IS NOT NULL
      AND birth_year BETWEEN 1966 AND 1986
),
unique_rows AS (
    -- Keep the latest row per country, plan
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        signup_date_plus_30d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country, plan ORDER BY birth_year DESC, id DESC) AS rn
        FROM cleaned_listeners
    ) ranked
    WHERE rn = 1
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        username,
        signup_date,
        birth_year,
        DATE_FORMAT(signup_date_plus_30d, 'yyyy-MM-dd') AS signup_date_plus_30d_label,
        DATEDIFF(signup_date_plus_30d, signup_date) AS days_signup_date_to_signup_date_plus_30d,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket
    FROM unique_rows
    WHERE UPPER(TRIM(username)) LIKE 'D%'
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE days_signup_date_to_signup_date_plus_30d > (SELECT AVG(days_signup_date_to_signup_date_plus_30d) FROM derived)
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS last_played_at
    FROM screened s
)
-- Final projection
SELECT
    id,
    username,
    signup_date,
    birth_year,
    signup_date_plus_30d_label,
    days_signup_date_to_signup_date_plus_30d,
    birth_year_bucket,
    last_played_at,
    ABS(birth_year_bucket - 1991) AS birth_year_bucket_distance
FROM with_counts
ORDER BY birth_year NULLS LAST, id
