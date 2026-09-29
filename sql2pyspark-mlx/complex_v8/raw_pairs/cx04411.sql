WITH cleaned AS (
    -- Scope the listeners rows
    SELECT
        id,
        country,
        birth_year,
        interests,
        CONCAT_WS('-', country, username) AS country_username_label
    FROM listeners
    WHERE signup_date >= DATE '2024-08-01'
      AND country = 'KR'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        country,
        birth_year,
        interests,
        country_username_label,
        ROUND(AVG(birth_year) OVER (PARTITION BY country), 2) AS country_avg_birth_year,
        ROUND(birth_year - AVG(birth_year) OVER (PARTITION BY country), 2) AS birth_year_vs_country_avg,
        COUNT(*) OVER (PARTITION BY country) AS rows_in_country,
        NTH_VALUE(birth_year, 2) OVER (PARTITION BY country ORDER BY birth_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_birth_year
    FROM cleaned
    WINDOW w_country AS (PARTITION BY country)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, rows_in_country, birth_year
    FROM with_trends
) src
UNPIVOT (
    reading FOR measure IN (rows_in_country, birth_year)
) unp
