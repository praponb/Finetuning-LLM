WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(location, 1, 4) AS location_prefix,
        ROUND(budget, 1) AS budget_rounded
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location NOT IN ('Singapore', 'Chiang Mai')
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.budget,
        s.location_prefix,
        x.salary
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(budget) IGNORE NULLS OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY salary, id) AS seq_no
    FROM latest_match
    WINDOW w_ordered AS (PARTITION BY dept_name ORDER BY salary DESC, id)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        budget,
        location_prefix,
        salary,
        first_known_budget,
        row_seq,
        ROUND(first_known_budget * 100, 2) AS adjusted_first_known_budget
    FROM scored
    WHERE dept_name IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    id,
    budget,
    location_prefix,
    first_known_budget,
    row_seq,
    adjusted_first_known_budget,
    AVG(adjusted_first_known_budget) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_adjusted_first_known_budget,
    SUM(adjusted_first_known_budget) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_adjusted_first_known_budget,
    COUNT(adjusted_first_known_budget) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_adjusted_first_known_budget,
    MIN(adjusted_first_known_budget) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_adjusted_first_known_budget
FROM valid
