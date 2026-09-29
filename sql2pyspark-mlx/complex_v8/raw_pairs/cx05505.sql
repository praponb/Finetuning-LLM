WITH base AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        country,
        birth_year
    FROM listeners
    WHERE plan = 'student'
      AND birth_year BETWEEN 1966 AND 1990
),
country_stats AS (
    -- Avg birth_year per country
    SELECT country, AVG(birth_year) AS avg_birth_year
    FROM base
    GROUP BY country
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_birth_year,
        ROUND(s.birth_year - g.avg_birth_year, 2) AS birth_year_minus_avg
    FROM base s
    JOIN country_stats g ON g.country = s.country
    WHERE s.birth_year > g.avg_birth_year
),
plan_stats AS (
    -- Median birth_year_minus_avg per plan
    SELECT plan, PERCENTILE_APPROX(birth_year_minus_avg, 0.5) AS median_birth_year_minus_avg
    FROM compared
    GROUP BY plan
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_birth_year_minus_avg,
        ROUND(s.birth_year_minus_avg - g.median_birth_year_minus_avg, 2) AS birth_year_minus_avg_minus_median
    FROM compared s
    JOIN plan_stats g ON g.plan = s.plan
    WHERE s.birth_year_minus_avg >= g.median_birth_year_minus_avg
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM vs_group
    WHERE birth_year_minus_avg > (SELECT AVG(birth_year_minus_avg) FROM vs_group)
),
totals AS (
    -- Group-level totals
    SELECT
        country,
        plan,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids
    FROM filtered
    GROUP BY country, plan
),
filtered_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE row_count > (SELECT AVG(row_count) FROM totals)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered_2
    WHERE row_count > (SELECT AVG(row_count) FROM filtered_2)
)
SELECT
    row_count,
    plan,
    distinct_ids,
    ARRAY_SORT(COLLECT_SET(country)) AS country_list
FROM screened
GROUP BY GROUPING SETS ((row_count, plan, distinct_ids), (row_count), (plan), ())
ORDER BY row_count NULLS LAST, plan NULLS LAST, distinct_ids NULLS LAST
