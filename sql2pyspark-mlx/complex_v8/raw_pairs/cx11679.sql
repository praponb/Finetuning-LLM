WITH filtered AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE (ARRAY_CONTAINS(interests, 'jazz') OR LENGTH(TRIM(username)) >= 5)
),
latest AS (
    -- Keep the earliest row per plan
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY plan ORDER BY signup_date, id) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
grouped AS (
    -- Group-level totals
    SELECT
        plan,
        ROUND(AVG(birth_year), 2) AS avg_birth_year,
        ROUND(STDDEV(birth_year), 2) AS stddev_birth_year,
        COUNT(DISTINCT plan) AS distinct_plans,
        COUNT(*) AS row_count
    FROM latest
    GROUP BY plan
    HAVING COUNT(*) >= 2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        plan,
        avg_birth_year,
        stddev_birth_year,
        distinct_plans,
        row_count,
        LEAD(distinct_plans, 3, 0) OVER (PARTITION BY row_count, distinct_plans ORDER BY row_count, plan) AS next3_distinct_plans,
        ROUND(avg_birth_year / NULLIF(SUM(avg_birth_year) OVER w_distinct_plans_row_count, 0), 4) AS avg_birth_year_share_of_distinct_plans_row_count,
        DENSE_RANK() OVER (PARTITION BY plan, row_count ORDER BY avg_birth_year ASC NULLS LAST) AS avg_birth_year_dense_rank,
        ROUND(AVG(row_count) OVER (PARTITION BY distinct_plans, row_count), 2) AS distinct_plans_row_count_avg_row_count,
        ROUND(stddev_birth_year / NULLIF(SUM(stddev_birth_year) OVER w_distinct_plans_row_count, 0), 4) AS stddev_birth_year_share_of_distinct_plans_row_co
    FROM grouped
    WINDOW w_distinct_plans_row_count AS (PARTITION BY distinct_plans, row_count)
),
top_rows AS (
    -- Top 3 per group
    SELECT
        plan,
        avg_birth_year,
        stddev_birth_year,
        distinct_plans,
        row_count,
        next3_distinct_plans,
        avg_birth_year_share_of_distinct_plans_row_count,
        distinct_plans_row_count_avg_row_count,
        stddev_birth_year_share_of_distinct_plans_row_co
    FROM with_trends
    WHERE avg_birth_year_dense_rank <= 3
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        row_count - stddev_birth_year AS row_count_minus_stddev_birth_year,
        CONCAT(plan, '#', CAST(distinct_plans AS STRING)) AS plan_ref
    FROM top_rows
    WHERE row_count < 9
)
-- Subtotals with GROUPING SETS
SELECT
    plan,
    distinct_plans,
    SUM(row_count) AS total_row_count,
    MAX(row_count_minus_stddev_birth_year) AS max_row_count_minus_stddev_birth_year,
    MAX(stddev_birth_year_share_of_distinct_plans_row_co) AS max_stddev_birth_year_share_of_distinct_plans_ro
FROM annotated
GROUP BY GROUPING SETS ((plan, distinct_plans), (plan), ())
