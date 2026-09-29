WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(level) AS level_lower
    FROM employees
    WHERE hire_date >= TO_DATE('2023-08-25')
      AND level != 'L3'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, dept_id ORDER BY hire_date DESC NULLS LAST, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
joined AS (
    -- Attach departments attributes
    SELECT
        s.*,
        d.budget,
        d.dept_name
    FROM ranked s
    JOIN departments d
        ON s.dept_id = d.id
    WHERE d.dept_name IN ('Legal', 'Engineering')
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        job_title,
        salary,
        is_remote,
        level_lower,
        rn,
        dept_name,
        STDDEV_SAMP(budget) OVER w_grp AS manager_id_stddev_samp_budget,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_cume_dist,
        PERCENT_RANK() OVER w_running AS budget_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY YEAR(hire_date), dept_id ORDER BY rn, id) AS row_seq
    FROM joined
    WINDOW w_grp AS (PARTITION BY manager_id), w_running AS (PARTITION BY job_title ORDER BY budget NULLS FIRST)
)
-- Window metrics over several different partitions
SELECT
    *,
    LAG(salary_cume_dist, 1) OVER w_grp AS prev_salary_cume_dist,
    SUM(manager_id_stddev_samp_budget) OVER (PARTITION BY job_title, dept_name) AS job_title_sum_manager_id_stddev_samp_budget,
    DENSE_RANK() OVER (PARTITION BY job_title ORDER BY budget_percent_rank DESC) AS budget_percent_rank_dense_rank,
    SUM(manager_id_stddev_samp_budget) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_manager_id_stddev_samp_budget
FROM scored
WINDOW w_grp AS (PARTITION BY dept_name, job_title ORDER BY salary_cume_dist, id);
