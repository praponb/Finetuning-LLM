WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST, id ASC) AS rn
        FROM departments
    ) ranked
    WHERE rn = 1
),
deduped_departments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC NULLS LAST, id ASC) AS latest_rank
        FROM deduped
    ) ranked
    WHERE latest_rank = 1
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.employee_hire_date,
        latest.employee_level
    FROM deduped_departments s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(budget) OVER w_hist AS cumulative_budget,
        SUM(budget) OVER (PARTITION BY location ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_2
    FROM with_last
    WINDOW w_hist AS (PARTITION BY employee_level ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT location, dept_name
    FROM scored
    EXCEPT DISTINCT
    SELECT location, dept_name
    FROM departments
    WHERE location = 'Chiang Mai'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.location = k.location AND s.dept_name = k.dept_name
),
scored_departments AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY employee_level ORDER BY cumulative_budget_2 DESC NULLS FIRST) AS cumulative_budget_2_dense_rank,
        DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY cumulative_budget_2 ASC NULLS LAST) AS cumulative_budget_2_dense_rank_2,
        ROUND(cumulative_budget / SUM(cumulative_budget) OVER (PARTITION BY employee_level), 4) AS cumulative_budget_share
    FROM verified
)
SELECT
    dept_name,
    location,
    budget,
    rn,
    latest_rank,
    employee_hire_date,
    employee_level,
    cumulative_budget_2,
    cumulative_budget_2_dense_rank
FROM scored_departments
ORDER BY cumulative_budget_2 ASC NULLS FIRST
