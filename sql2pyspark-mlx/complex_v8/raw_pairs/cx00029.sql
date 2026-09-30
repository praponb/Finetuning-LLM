WITH prepared AS (
    -- Scope the listeners rows
    SELECT
        id,
        plan,
        signup_date,
        birth_year,
        interests,
        ADD_MONTHS(signup_date, 12) AS signup_date_plus_12m
    FROM listeners
    WHERE NOT ARRAY_CONTAINS(interests, 'kpop')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        birth_year - LAG(birth_year) OVER (ORDER BY birth_year, id) AS birth_year_change,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date_plus_12m) ORDER BY signup_date_plus_12m DESC, id) AS signup_date_plus_12m_quintile,
        MAX(birth_year) OVER (PARTITION BY YEAR(signup_date), DATE_TRUNC('MONTH', signup_date_plus_12m)) AS signup_date_year_signup_date_plus_12m_month_max_
    FROM prepared
),
plan_stats AS (
    -- Avg birth_year per plan
    SELECT plan, AVG(birth_year) AS avg_birth_year
    FROM with_trends
    GROUP BY plan
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_birth_year,
        ROUND(s.birth_year - g.avg_birth_year, 2) AS birth_year_minus_avg
    FROM with_trends s
    JOIN plan_stats g ON g.plan = s.plan
    WHERE s.birth_year > g.avg_birth_year
),
exploded AS (
    -- One row per interest
    SELECT
        s.id,
        s.plan,
        s.signup_date,
        s.birth_year,
        s.signup_date_plus_12m,
        s.birth_year_change,
        s.signup_date_plus_12m_quintile,
        s.signup_date_year_signup_date_plus_12m_month_max_,
        s.avg_birth_year,
        s.birth_year_minus_avg,
        t.interest
    FROM vs_group s
    LATERAL VIEW EXPLODE(s.interests) t AS interest
),
labeled AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(plan), 1, 3) AS plan_prefix
    FROM exploded
)
SELECT id, interest, metric_name, metric_value
FROM (
    SELECT id, interest, birth_year_minus_avg, avg_birth_year
    FROM labeled
) src
UNPIVOT (
    metric_value FOR metric_name IN (birth_year_minus_avg AS m_birth_year_minus_avg, avg_birth_year AS avg_birth_year_v)
) unp
