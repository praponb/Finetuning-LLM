WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(job_title, 'N/A') AS job_title_clean
    FROM employees
    WHERE hire_date > DATE '2023-05-28'
      AND level = 'L2'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(salary) OVER (PARTITION BY manager_id) AS manager_id_max_salary,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY is_remote_flag ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_avg_5_prior
    FROM cleaned
),
kept_keys AS (
    -- Set difference on keys
    SELECT manager_id
    FROM enriched
    EXCEPT ALL
    SELECT manager_id
    FROM employees
    WHERE email IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.manager_id = k.manager_id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LEAST(manager_id_max_salary, salary_avg_5_prior) AS least_manager_id_max_salary_salary_avg_5_prior
    FROM kept_rows
    WHERE salary_avg_5_prior > 94063
),
enriched_employees AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        is_remote_flag,
        job_title_clean,
        manager_id_max_salary,
        salary_avg_5_prior,
        least_manager_id_max_salary_salary_avg_5_prior,
        RANK() OVER (PARTITION BY level ORDER BY is_remote_flag ASC) AS is_remote_flag_rank,
        CUME_DIST() OVER (ORDER BY least_manager_id_max_salary_salary_avg_5_prior ASC) AS least_manager_id_max_salary_salary_avg_5_prior_cume_dist
    FROM valid
),
joined AS (
    -- Attach departments attributes
    SELECT
        e.*,
        d.location,
        d.budget
    FROM enriched_employees e
    INNER JOIN departments d
        ON e.dept_id = d.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN salary >= 154000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
        DATE_TRUNC('YEAR', hire_date) AS year_start,
        APPROX_PERCENTILE(least_manager_id_max_salary_salary_avg_5_prior, ARRAY(0.25, 0.5, 0.75)) AS least_manager_id_max_salary_salary_avg_5_prior_quartiles,
        COUNT(*) FILTER (WHERE location = 'Tokyo') AS tokyo_rows,
        PERCENTILE_APPROX(manager_id_max_salary, ARRAY(0.25, 0.5, 0.75)) AS manager_id_max_salary_quartiles,
        COUNT(CASE WHEN job_title = 'Analyst' THEN 1 END) AS analyst_count
    FROM joined
    GROUP BY CASE WHEN salary >= 154000 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', hire_date)
)
SELECT
    salary_tier,
    year_start,
    tokyo_rows,
    manager_id_max_salary_quartiles,
    analyst_count
FROM daily
ORDER BY year_start ASC, salary_tier ASC
