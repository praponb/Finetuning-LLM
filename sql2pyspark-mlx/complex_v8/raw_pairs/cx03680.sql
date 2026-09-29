WITH cleaned_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        signup_date,
        birth_year,
        interests,
        LAST_DAY(signup_date) AS signup_date_month_end,
        SUBSTRING(TRIM(username), 1, 2) AS username_prefix
    FROM listeners
    WHERE birth_year >= 1975
      AND plan IN ('free', 'family', 'premium')
),
exploded AS (
    -- One row per interest
    SELECT
        s.id,
        s.username,
        s.plan,
        s.signup_date,
        s.birth_year,
        s.signup_date_month_end,
        s.username_prefix,
        t.interest
    FROM cleaned_listeners s
    LATERAL VIEW EXPLODE(s.interests) t AS interest
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(username_prefix)) AS username_prefix_len,
        CONCAT_WS('-', interest, username) AS interest_username_label
    FROM exploded
)
SELECT *
FROM (
    SELECT interest, plan, username_prefix_len
    FROM derived
) src
PIVOT (
    COUNT(username_prefix_len)
    FOR plan IN ('free', 'family', 'premium')
)
ORDER BY interest
