WITH valid AS (
    SELECT
        *,
        COALESCE(location, 'unassigned') AS location_filled,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(dept_name, 'N/A') AS dept_name_clean
    FROM departments
    WHERE dept_name IS NOT NULL
      AND budget BETWEEN 2271000 AND 2387000
),
cleaned AS (
    SELECT *, CONCAT(dept_name, ' ', location) AS dept_name_location
    FROM valid
    WHERE location IS NOT NULL
),
scored AS (
    SELECT
        *,
        FIRST_VALUE(budget_filled, TRUE) OVER (PARTITION BY dept_name ORDER BY budget_filled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget_filled,
        SUM(budget) OVER (ORDER BY budget_filled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget ASC NULLS LAST) AS budget_percent_rank
    FROM cleaned
),
cleaned_departments AS (
    SELECT *, SUBSTRING(location, 1, 4) AS location_prefix
    FROM scored
    WHERE budget BETWEEN 2348000 AND 3317000
),
not_flagged AS (
    SELECT s.*
    FROM cleaned_departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id IN (11, 26, 28)
        )
),
enriched AS (
    SELECT
        id,
        dept_name,
        location,
        dept_name_clean,
        dept_name_location,
        budget_percent_rank,
        LAG(budget_filled) OVER (PARTITION BY location ORDER BY first_known_budget_filled ASC, id ASC) AS prev_budget_filled,
        LEAD(budget_filled) OVER (PARTITION BY location ORDER BY first_known_budget_filled ASC, id ASC) AS next_budget_filled,
        LEAD(budget) OVER (PARTITION BY location ORDER BY first_known_budget_filled ASC, id ASC) AS next_budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY first_known_budget_filled ASC, id ASC) AS seq_no,
        MIN(cumulative_budget) OVER (PARTITION BY dept_name) AS dept_name_min_cumulative_budget
    FROM not_flagged
    WHERE budget_percent_rank BETWEEN 0.77 AND 0.79
)
SELECT
    id,
    dept_name,
    location,
    dept_name_clean,
    dept_name_location,
    budget_percent_rank,
    prev_budget_filled,
    next_budget,
    seq_no,
    CASE WHEN dept_name RLIKE '^(Finance|Sales)$' THEN 'GROUP_B' WHEN dept_name RLIKE '^(Engineering|Marketing)$' THEN 'CORE' ELSE 'PRIORITY' END AS dept_name_zone
FROM enriched
