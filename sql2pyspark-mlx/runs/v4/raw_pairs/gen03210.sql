WITH valid AS (
    SELECT *, SUBSTRING(location, 1, 3) AS location_prefix
    FROM departments
    WHERE location IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY location, dept_name ORDER BY budget DESC NULLS FIRST) AS rn
        FROM valid
    ) x
    WHERE rn = 1
),
metrics AS (
    SELECT
        *,
        AVG(budget) OVER (PARTITION BY location ORDER BY rn, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS budget_avg_2,
        NTILE(3) OVER w_ordered AS budget_bucket,
        COUNT(budget) OVER (PARTITION BY dept_name) AS dept_name_count_budget
    FROM latest
    WINDOW w_ordered AS (PARTITION BY location, dept_name ORDER BY budget DESC, id)
)
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget
    FROM metrics
) src
PIVOT (
    SUM(budget) AS amount,
    COUNT(1) AS item_count
    FOR location IN ('Singapore', 'Berlin')
);
