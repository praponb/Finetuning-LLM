WITH cleaned_listeners AS (
    -- Filter listeners
    SELECT *
    FROM listeners
    WHERE signup_date IS NOT NULL
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        ABS(birth_year - 1977) AS birth_year_distance
    FROM cleaned_listeners
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_date_ym,
        ARRAY_SORT(COLLECT_SET(username)) AS username_list,
        SUM(birth_year_distance) AS total_birth_year_distance
    FROM annotated
    GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
    HAVING MAX(birth_year) > 1970
)
SELECT
    signup_date_ym,
    total_birth_year_distance,
    INITCAP(TRIM(signup_date_ym)) AS signup_date_ym_clean
FROM rollup_base
ORDER BY signup_date_ym_clean, signup_date_ym NULLS LAST
