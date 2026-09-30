WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(salary AS INT) AS salary_int,
        ROUND(salary * 1.15, 2) AS salary_adjusted
    FROM employees
    WHERE hire_date >= DATE '2023-02-05'
      AND level NOT IN ('L5', 'L2', 'L3')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY salary, id) AS row_seq,
        LEAD(salary_adjusted, 1) OVER w_recent AS next_salary_adjusted
    FROM valid
    WINDOW w_recent AS (ORDER BY salary_adjusted, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        job_title,
        salary,
        is_remote,
        level,
        salary_int,
        next_salary_adjusted,
        NTILE(4) OVER (PARTITION BY YEAR(hire_date), level ORDER BY salary_adjusted DESC, id) AS salary_adjusted_bucket,
        NTILE(10) OVER (PARTITION BY job_title, level ORDER BY salary DESC, id) AS salary_bucket
    FROM scored
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        level,
        next_salary_adjusted,
        salary_adjusted_bucket,
        RANK() OVER (PARTITION BY job_title ORDER BY salary_int DESC) AS salary_int_rank,
        AVG(salary) OVER (PARTITION BY job_title ORDER BY next_salary_adjusted, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior,
        SUM(next_salary_adjusted) OVER (PARTITION BY level) AS level_sum_next_salary_adjusted
    FROM with_windows
)
SELECT
    id,
    level,
    salary_adjusted_bucket,
    salary_int_rank,
    CASE WHEN level RLIKE '^(L3|L2)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS level_zone
FROM scored_employees
ORDER BY id NULLS LAST, salary_adjusted_bucket;
