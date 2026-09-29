WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(is_remote AS INT) AS is_remote_flag,
        UPPER(full_name) AS full_name_upper
    FROM employees
    WHERE UPPER(full_name) LIKE '%RAN%'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        email,
        manager_id,
        salary,
        hire_date,
        level
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date NULLS FIRST, id) AS dedup_rank
        FROM filtered
    ) x
    WHERE dedup_rank = 1
),
ranked_employees AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY salary, id) AS rank_in_group
        FROM ranked
    ) t
    WHERE rank_in_group = 1
),
enriched AS (
    -- Attach employees attributes
    SELECT
        a.*,
        d.level AS employee_level,
        d.hire_date AS employee_hire_date
    FROM ranked_employees a
    INNER JOIN employees d
        ON a.manager_id <=> d.id
    WHERE d.manager_id IN (2, 17, 40)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER (ORDER BY salary DESC, id) AS salary_bucket,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('WEEK', hire_date), level ORDER BY salary, id) AS salary_bucket_2,
        SUM(salary) OVER (PARTITION BY manager_id, level ORDER BY employee_hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('week', employee_hire_date), employee_level ORDER BY salary, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY MONTH(employee_hire_date), level ORDER BY rank_in_group, id) AS seq_no
    FROM enriched
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN running_salary >= 638676 THEN 'HIGH' ELSE 'LOW' END AS running_salary_tier,
        employee_level,
        COUNT(CASE WHEN employee_level = 'L2' THEN 1 END) AS l2_count,
        SORT_ARRAY(COLLECT_SET(level)) AS level_list,
        SUM(row_seq) FILTER (WHERE level = 'L4') AS l4_total_row_seq,
        MAX(seq_no) FILTER (WHERE employee_level = 'L2') AS l2_max_seq_no,
        COUNT(DISTINCT level) AS distinct_levels,
        COUNT(CASE WHEN level = 'L5' THEN 1 END) AS l5_count,
        GROUPING_ID() AS grouping_level
    FROM with_windows
    GROUP BY GROUPING SETS ((CASE WHEN running_salary >= 638676 THEN 'HIGH' ELSE 'LOW' END, employee_level), (CASE WHEN running_salary >= 638676 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT
    running_salary_tier,
    level_list,
    l4_total_row_seq,
    l2_max_seq_no,
    l5_count,
    grouping_level
FROM summary
ORDER BY l2_max_seq_no DESC NULLS LAST
