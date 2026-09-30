WITH cleaned AS (
    SELECT
        id,
        full_name,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        UPPER(TRIM(email)) AS email_normalized,
        COALESCE(job_title, 'DIRECT') AS job_title_clean
    FROM employees
    WHERE hire_date >= TO_DATE('2023-08-22')
      AND LENGTH(email) > 12
),
kept_keys AS (
    SELECT id, level
    FROM cleaned
    EXCEPT
    SELECT id, level
    FROM employees
    WHERE manager_id IN (3, 39)
),
reconciled AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id AND s.level = k.level
),
enriched AS (
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share_2,
        MAX(salary) OVER (PARTITION BY job_title) AS job_title_max_salary
    FROM reconciled
    WHERE NOT is_remote
),
scored AS (
    SELECT
        full_name,
        job_title,
        salary,
        hire_date,
        level,
        job_title_clean,
        salary_share,
        salary_share_2,
        COUNT(job_title_max_salary) OVER (PARTITION BY job_title) AS job_title_count_job_title_max_salary,
        SUM(salary_share) OVER (PARTITION BY manager_id) AS manager_id_sum_salary_share
    FROM enriched
)
SELECT
    full_name,
    job_title,
    level,
    job_title_clean,
    salary_share,
    salary_share_2,
    job_title_count_job_title_max_salary
FROM scored
ORDER BY job_title_count_job_title_max_salary DESC, job_title DESC;
