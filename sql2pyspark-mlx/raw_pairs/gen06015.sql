WITH cleaned AS (
    SELECT
        *,
        CONCAT(dept_name, ' - ', location) AS dept_name_location,
        UPPER(dept_name) AS dept_name_upper,
        COALESCE(dept_name, 'NONE') AS dept_name_clean
    FROM departments
    WHERE location IS NOT NULL
      AND dept_name IS DISTINCT FROM 'Legal'
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
),
base AS (
    SELECT *, CAST(rank_in_group AS STRING) AS rank_in_group_str
    FROM ranked
    WHERE budget BETWEEN 1215000 AND 3522000
),
enriched AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_upper,
        dept_name_clean,
        rank_in_group,
        SUM(budget) OVER w_recent AS running_sum_budget,
        AVG(budget) OVER w_recent AS running_avg_budget,
        MIN(budget) OVER w_recent AS running_min_budget,
        COUNT(budget) OVER w_recent AS running_count_budget
    FROM base
    WINDOW w_recent AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    dept_name,
    measure,
    value
FROM (
    SELECT
        id,
        dept_name,
        running_sum_budget,
        running_min_budget,
        budget,
        running_avg_budget
    FROM enriched
) src
UNPIVOT (
    value FOR measure IN (running_sum_budget, running_min_budget AS RUNNING_MIN_BUDGET, budget, running_avg_budget)
);
