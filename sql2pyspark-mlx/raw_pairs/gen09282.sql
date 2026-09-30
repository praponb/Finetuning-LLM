WITH scored AS (
    SELECT
        id,
        dept_name,
        MAX(budget) OVER w_grp last3_max_budget,
        COUNT(budget) OVER w_grp last3_count_budget,
        MIN(budget) OVER w_grp last3_min_budget,
        LEAD(budget, 1) OVER w_part next_budget
    FROM departments
    WINDOW w_grp AS (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_part AS (ORDER BY budget, id)
),
kept_keys AS (
    SELECT id, dept_name
    FROM scored
    EXCEPT
    SELECT id, dept_name
    FROM departments
    WHERE dept_name IN ('Engineering', 'Marketing')
),
verified AS (
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.id = k.id AND s.dept_name = k.dept_name
),
base AS (
    SELECT
        id,
        dept_name,
        last3_max_budget,
        last3_min_budget,
        next_budget,
        COALESCE(last3_count_budget, 0) last3_count_budget_filled,
        SUBSTRING(dept_name, 1, 4) dept_name_prefix,
        COALESCE(dept_name, 'NONE') dept_name_filled
    FROM verified
    WHERE last3_max_budget IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id IN (6, 9, 19, 20)
        )
)
SELECT
    CASE WHEN last3_count_budget_filled >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_count_budget_filled_tier,
    APPROX_PERCENTILE(last3_count_budget_filled, ARRAY(0.25, 0.5, 0.75)) AS last3_count_budget_filled_quartiles,
    COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names
FROM kept
GROUP BY 1
ORDER BY hr_count NULLS FIRST
