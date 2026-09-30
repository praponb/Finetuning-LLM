WITH filtered AS (
    SELECT
        *,
        CASE WHEN dept_name IN ('Sales', 'Finance') THEN 'Core' ELSE 'Other' END AS dept_name_group,
        COALESCE(location, 'UNKNOWN') AS location_filled
    FROM departments
    WHERE dept_name IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND NOT ch.is_remote
        )
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget NULLS LAST, id) AS row_num
    FROM not_flagged
    QUALIFY row_num = 1
),
not_flagged_departments AS (
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id IS NULL
        )
),
eligible AS (
    SELECT s.*
    FROM not_flagged_departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date <= '2023-03-01'
        )
),
latest AS (
    SELECT *
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC NULLS FIRST, id) = 1
),
with_windows AS (
    SELECT
        *,
        LAG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id) AS prev_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS seq_no
    FROM latest
)
SELECT
    CASE WHEN budget >= 4487000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    dept_name,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    COUNT_IF(location = 'Tokyo') AS tokyo_events,
    AVG(seq_no) FILTER (WHERE dept_name = 'HR') AS hr_avg_seq_no,
    COUNT(*) FILTER (WHERE dept_name = 'HR') AS hr_rows,
    COUNT(*) FILTER (WHERE dept_name = 'HR') AS hr_rows_2,
    COUNT_IF(dept_name = 'Marketing') AS marketing_events,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN budget >= 4487000 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(AVG(seq_no), 2) DESC) AS avg_seq_no_percent_rank
FROM with_windows
GROUP BY budget_tier, dept_name
HAVING hr_avg_seq_no < 1000000
ORDER BY hr_rows;
