WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(email) AS email_lower,
        COALESCE(level, 'unassigned') AS level_filled
    FROM employees
    WHERE manager_id IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        email,
        dept_id,
        salary,
        hire_date,
        is_remote,
        level,
        email_lower,
        level_filled
    FROM (
        SELECT *, RANK() OVER (PARTITION BY job_title, manager_id ORDER BY hire_date DESC) AS latest_rank
        FROM filtered
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.dept_id,
        a.salary,
        a.hire_date,
        a.level_filled,
        p.dept_name,
        p.budget
    FROM deduped a
    LEFT JOIN departments p
        ON a.dept_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        budget,
        COUNT(budget) OVER w_ordered AS running_count_budget,
        MIN(budget) OVER w_ordered AS running_min_budget,
        MAX(budget) OVER w_ordered AS running_max_budget
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY dept_id, dept_name ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    budget,
    running_min_budget,
    running_max_budget
FROM scored
ORDER BY running_min_budget DESC NULLS FIRST, id
