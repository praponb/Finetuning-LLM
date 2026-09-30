WITH valid AS (
    -- Filter listeners
    SELECT
        id,
        country,
        signup_date,
        birth_year,
        interests,
        LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug
    FROM listeners
    WHERE UPPER(TRIM(username)) LIKE 'A%'
      AND plan NOT IN ('premium', 'free')
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', signup_date) AS signup_date_month,
        country,
        COUNT(DISTINCT country) AS distinct_countrys,
        MIN(signup_date) AS first_signup_date,
        ROUND(STDDEV(birth_year), 2) AS stddev_birth_year
    FROM valid
    GROUP BY 1, 2
),
derived AS (
    -- Label and score rows
    SELECT
        signup_date_month,
        country,
        distinct_countrys,
        first_signup_date,
        stddev_birth_year,
        CASE WHEN country IN ('JP', 'TH') THEN 'group_a' WHEN country = 'BR' THEN 'group_b' ELSE 'other' END AS country_group
    FROM per_group
    WHERE stddev_birth_year <= 1506.0
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        signup_date_month,
        country,
        stddev_birth_year,
        country_group,
        MIN(stddev_birth_year) OVER (ORDER BY stddev_birth_year ASC, signup_date_month, country ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_stddev_birth_year_so_far,
        MAX(distinct_countrys) OVER (PARTITION BY distinct_countrys) AS distinct_countrys_max_distinct_countrys,
        SUM(distinct_countrys) OVER (ORDER BY stddev_birth_year ASC, signup_date_month, country ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS distinct_countrys_sum_7,
        ROUND(AVG(stddev_birth_year) OVER (PARTITION BY country_group ORDER BY distinct_countrys ASC, signup_date_month, country ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS stddev_birth_year_avg_6_prior,
        SUM(distinct_countrys) OVER (ORDER BY stddev_birth_year ASC, signup_date_month, country ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS distinct_countrys_sum_6_prior
    FROM derived
)
-- Native SQL UNPIVOT (wide to long)
SELECT signup_date_month, country, field, value
FROM (
    SELECT signup_date_month, country, distinct_countrys_max_distinct_countrys, distinct_countrys_sum_7, distinct_countrys_sum_6_prior
    FROM ranked
) src
UNPIVOT (
    value FOR field IN (distinct_countrys_max_distinct_countrys AS distinct_countrys_max_distinct_countrys_v, distinct_countrys_sum_7 AS m_distinct_countrys_sum_7, distinct_countrys_sum_6_prior AS m_distinct_countrys_sum_6_prior)
) unp
