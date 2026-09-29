WITH valid AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        level,
        CONCAT(email, ', ', level) AS email_level,
        CASE WHEN salary >= 143000 THEN 'High' WHEN salary >= 71000 THEN 'Medium' ELSE 'Low' END AS salary_level
    FROM employees
    WHERE hire_date >= DATE '2023-08-12'
),
metrics AS (
    SELECT
        id,
        email,
        dept_id,
        job_title,
        salary,
        email_level,
        salary_level,
        ROW_NUMBER() OVER (PARTITION BY level, dept_id ORDER BY hire_date DESC, id) AS row_seq,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) AS salary_share
    FROM valid
),
kept_keys AS (
    SELECT job_title
    FROM metrics
    EXCEPT
    SELECT job_title
    FROM employees
    WHERE salary BETWEEN 79000 AND 144000
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.job_title = k.job_title
),
kept_keys_2 AS (
    SELECT dept_id
    FROM reconciled
    EXCEPT
    SELECT dept_id
    FROM employees
    WHERE is_remote
),
verified AS (
    SELECT s.*
    FROM reconciled s
    INNER JOIN kept_keys_2 k ON s.dept_id = k.dept_id
),
enriched AS (
    SELECT
        id,
        email,
        job_title,
        salary_level,
        salary_share,
        DENSE_RANK() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_dense_rank,
        RANK() OVER (ORDER BY row_seq NULLS FIRST) AS row_seq_rank
    FROM verified
    WHERE email NOT LIKE '%@company.co'
)
SELECT
    id,
    email,
    salary_level,
    salary_dense_rank,
    row_seq_rank
FROM enriched
ORDER BY row_seq_rank DESC NULLS FIRST
