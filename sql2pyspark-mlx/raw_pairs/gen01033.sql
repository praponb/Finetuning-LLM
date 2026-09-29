WITH kept AS (
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id IS NOT NULL
        )
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS LAST, id) AS rank_in_group
    FROM kept
    QUALIFY rank_in_group = 1
),
deduped AS (
    SELECT
        id,
        dept_name,
        budget
    FROM (
        SELECT *, RANK() OVER (PARTITION BY location ORDER BY rank_in_group DESC) AS row_num
        FROM latest
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    SELECT
        *,
        MAX(budget) OVER (PARTITION BY dept_name) AS dept_name_max_budget,
        LEAD(budget, 1) OVER (PARTITION BY dept_name ORDER BY budget, id) AS next_budget,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS budget_avg_2,
        LAST_VALUE(dept_name, TRUE) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name
    FROM deduped
    WHERE budget IS NULL
),
scored AS (
    SELECT
        id,
        dept_name,
        budget,
        dept_name_max_budget,
        next_budget,
        budget_avg_2,
        RANK() OVER (PARTITION BY dept_name ORDER BY dept_name_max_budget DESC NULLS LAST) AS dept_name_max_budget_rank,
        NTILE(5) OVER (PARTITION BY dept_name ORDER BY next_budget DESC, id) AS next_budget_bucket,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget
    FROM with_windows
),
with_windows_departments AS (
    SELECT
        *,
        FIRST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY dept_name_max_budget_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name,
        SUM(dept_name_max_budget_rank) OVER w_running AS cumulative_dept_name_max_budget_rank
    FROM scored
    WINDOW w_running AS (PARTITION BY dept_name ORDER BY next_budget_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT
        id,
        dept_name,
        budget,
        next_budget,
        dept_name_max_budget_rank,
        next_budget_bucket,
        cumulative_budget,
        first_known_dept_name,
        cumulative_dept_name_max_budget_rank,
        GREATEST(next_budget, cumulative_budget) AS greatest_next_budget_cumulative_budget,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_filled
    FROM with_windows_departments
    WHERE next_budget IS NULL
)
SELECT
    dept_name,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list_2
FROM cleaned
GROUP BY 1
HAVING COUNT(DISTINCT dept_name) > 1;
