WITH base AS (
    SELECT
        id,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        IF(is_remote, 'Yes', 'No') AS is_remote_label,
        COALESCE(level, 'UNKNOWN') AS level_filled
    FROM employees
    WHERE level IS NOT DISTINCT FROM 'L2'
),
filtered AS (
    SELECT
        id,
        dept_id,
        salary,
        hire_date,
        level,
        is_remote_label,
        level_filled,
        CAST(is_remote AS INT) AS is_remote_flag
    FROM base
    WHERE hire_date > TO_DATE('2023-02-14')
),
scored AS (
    SELECT
        *,
        LEAD(is_remote_flag, 1) OVER (PARTITION BY dept_id ORDER BY salary, id) AS next_is_remote_flag,
        AVG(is_remote_flag) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS is_remote_flag_avg_2
    FROM filtered
),
metrics AS (
    SELECT
        *,
        MAX(is_remote_flag_avg_2) OVER (PARTITION BY level, dept_id ORDER BY next_is_remote_flag, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_is_remote_flag_avg_2,
        MIN(is_remote_flag_avg_2) OVER (PARTITION BY level, dept_id ORDER BY next_is_remote_flag, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_is_remote_flag_avg_2,
        SUM(is_remote_flag_avg_2) OVER (PARTITION BY level, dept_id ORDER BY next_is_remote_flag, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_is_remote_flag_avg_2,
        AVG(is_remote_flag_avg_2) OVER (PARTITION BY level, dept_id ORDER BY next_is_remote_flag, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_is_remote_flag_avg_2
    FROM scored
),
with_dims AS (
    SELECT
        a.id,
        a.dept_id,
        a.salary,
        a.hire_date,
        a.is_remote_label,
        a.level_filled,
        a.next_is_remote_flag,
        a.last4_max_is_remote_flag_avg_2,
        a.last4_sum_is_remote_flag_avg_2,
        a.last4_avg_is_remote_flag_avg_2,
        b.dept_name
    FROM metrics a
    LEFT JOIN departments b
        ON a.dept_id = b.id
),
kept_keys AS (
    SELECT dept_id, id
    FROM with_dims
    INTERSECT
    SELECT dept_id, id
    FROM employees
    WHERE NOT is_remote
),
verified AS (
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.dept_id = k.dept_id AND s.id = k.id
)
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    COUNT(*) AS row_count,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    ROUND(AVG(last4_max_is_remote_flag_avg_2), 2) AS avg_last4_max_is_remote_flag_avg_2
FROM verified
GROUP BY 1
HAVING ROUND(AVG(last4_max_is_remote_flag_avg_2), 2) < 1000000;
