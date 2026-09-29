WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(level, 'NONE') AS level_clean
    FROM employees
    WHERE hire_date >= TO_DATE('2023-05-11')
      AND manager_id IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        is_remote,
        COUNT(salary) OVER (PARTITION BY job_title) AS job_title_count_salary,
        AVG(salary) OVER (PARTITION BY job_title ORDER BY hire_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS salary_avg_2
    FROM cleaned
),
joined AS (
    -- Attach employees attributes
    SELECT
        s.*,
        e.salary AS employee_salary,
        e.job_title AS employee_job_title,
        e.hire_date
    FROM enriched s
    LEFT JOIN employees e
        ON s.manager_id = e.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT job_title, id
    FROM joined
    EXCEPT ALL
    SELECT job_title, id
    FROM employees
    WHERE NOT is_remote
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN remaining_keys k ON s.job_title = k.job_title AND s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY job_title_count_salary ASC NULLS FIRST) AS job_title_count_salary_cume_dist,
        ROUND(salary_avg_2 / SUM(salary_avg_2) OVER (PARTITION BY manager_id), 4) AS salary_avg_2_share,
        ROUND(job_title_count_salary / SUM(job_title_count_salary) OVER (PARTITION BY employee_job_title), 4) AS job_title_count_salary_share
    FROM verified
    WHERE NOT is_remote
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            job_title,
            employee_job_title,
            salary_avg_2
        FROM scored
    ) src
    PIVOT (
        SUM(salary_avg_2) AS amount,
        COUNT(1) AS item_count
        FOR employee_job_title IN ('Engineer', 'Director')
    )
)
SELECT
    job_title,
    Engineer_amount,
    Engineer_item_count,
    Director_amount,
    Director_item_count
FROM by_type;
