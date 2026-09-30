WITH filtered AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        COALESCE(birth_year, 0) AS birth_year_filled
    FROM listeners
    WHERE (birth_year >= 1969 OR country IN ('UK', 'TH', 'BR'))
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        COUNT(*) OVER w_country AS rows_in_country,
        MIN(birth_year) OVER (PARTITION BY country, MONTH(signup_date) ORDER BY signup_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_birth_year_so_far,
        birth_year_filled - LAG(birth_year_filled) OVER (PARTITION BY country, MONTH(signup_date) ORDER BY signup_date ASC, id) AS birth_year_filled_change,
        FIRST_VALUE(signup_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date), plan ORDER BY birth_year_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_signup_date,
        MAX(birth_year_filled) OVER (PARTITION BY country) AS country_max_birth_year_filled,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER (PARTITION BY country), 0), 4) AS birth_year_share_of_country,
        ROUND(AVG(birth_year_filled) OVER w_country, 2) AS country_avg_birth_year_filled,
        ROUND(CUME_DIST() OVER (PARTITION BY country, MONTH(signup_date) ORDER BY birth_year DESC NULLS FIRST, id), 3) AS birth_year_cume_dist
    FROM filtered
    WINDOW w_country AS (PARTITION BY country)
)
-- Final projection
SELECT
    id,
    username,
    signup_date,
    birth_year,
    rows_in_country,
    min_birth_year_so_far,
    first_known_signup_date,
    country_max_birth_year_filled,
    birth_year_share_of_country,
    country_avg_birth_year_filled,
    birth_year_cume_dist,
    GREATEST(birth_year_filled_change, birth_year_cume_dist) AS greatest_birth_year_filled_change_birth_year_cum
FROM windowed
ORDER BY min_birth_year_so_far, rows_in_country, id
LIMIT 20
