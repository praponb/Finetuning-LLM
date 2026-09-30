WITH valid_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE birth_year > 1969
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM valid_listeners s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
        )
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        MONTH(signup_date) AS signup_date_month,
        DATE_FORMAT(signup_date, 'yyyy-MM-dd') AS signup_date_label
    FROM filtered
    WHERE birth_year <= 1994
)
-- Subtotals with GROUPING SETS
SELECT
    country,
    plan,
    SUM(birth_year) AS total_birth_year,
    ARRAY_SORT(COLLECT_SET(username)) AS username_list,
    COUNT(DISTINCT plan) AS distinct_plans
FROM enriched
GROUP BY GROUPING SETS ((country, plan), (country), (plan))
ORDER BY country NULLS LAST, plan NULLS LAST
