WITH valid_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        ADD_MONTHS(signup_date, 1) AS signup_date_plus_1m,
        UPPER(TRIM(country)) AS country_clean,
        INITCAP(TRIM(country)) AS country_clean_2
    FROM listeners
    WHERE signup_date >= DATE '2024-08-02'
      AND SIZE(interests) >= 1
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS first_played_at,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS first_played_at_2
    FROM valid_listeners s
),
country_stats AS (
    -- Max birth_year per country
    SELECT country, MAX(birth_year) AS max_birth_year
    FROM profiled
    GROUP BY country
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_birth_year,
        ROUND(s.birth_year - g.max_birth_year, 2) AS birth_year_minus_max
    FROM profiled s
    JOIN country_stats g ON g.country = s.country
    WHERE s.birth_year = g.max_birth_year
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        signup_date_plus_1m,
        country_clean_2,
        first_played_at,
        max_birth_year,
        birth_year_minus_max,
        UPPER(TRIM(username)) AS username_clean,
        ROUND((UNIX_TIMESTAMP(first_played_at_2) - UNIX_TIMESTAMP(first_played_at)) / 3600, 1) AS hours_first_played_at_to_first_played_at_2,
        MONTH(signup_date) AS signup_date_month
    FROM compared
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(country, 'ALL') AS country,
    COALESCE(plan, 'ALL') AS plan,
    signup_date_month,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE country NOT IN ('UK', 'TH')) AS n_country_not_in_uk_th
FROM annotated
GROUP BY GROUPING SETS ((country, plan, signup_date_month), (country), (plan), ())
