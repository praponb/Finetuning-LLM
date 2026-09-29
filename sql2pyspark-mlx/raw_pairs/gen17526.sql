WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS latest_rank
        FROM employees
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(salary) OVER w_grp AS next_salary,
        LAG(salary) OVER w_grp AS prev_salary,
        ROW_NUMBER() OVER w_grp AS row_seq,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share
    FROM deduped
    WHERE dept_id = 35
    WINDOW w_grp AS (PARTITION BY manager_id ORDER BY salary, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        latest_rank,
        next_salary,
        prev_salary,
        LAG(salary_share, 1) OVER (PARTITION BY dept_id ORDER BY latest_rank, id) AS prev_salary_share,
        LAG(salary, 1) OVER (ORDER BY latest_rank, id) AS prev_salary_2,
        FIRST_VALUE(salary_share) IGNORE NULLS OVER (PARTITION BY level, manager_id ORDER BY salary_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_salary_share
    FROM metrics
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY first_known_salary_share DESC NULLS FIRST, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        manager_id,
        job_title,
        is_remote,
        level,
        latest_rank,
        prev_salary,
        prev_salary_2,
        rank_in_group,
        LEAD(prev_salary) OVER w_part AS next_prev_salary,
        LAG(prev_salary) OVER w_part AS prev_prev_salary,
        LEAD(prev_salary_2) OVER w_part AS next_prev_salary_2,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM latest
    WINDOW w_part AS (PARTITION BY level, manager_id ORDER BY prev_salary_2, id)
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN prev_salary >= 138000 THEN 'LARGE' ELSE 'SMALL' END AS prev_salary_tier,
        job_title,
        ROUND(AVG(next_prev_salary_2), 2) AS avg_next_prev_salary_2,
        SIZE(COLLECT_SET(level)) AS num_levels,
        COUNT(CASE WHEN level = 'L3' THEN 1 END) AS l3_count,
        APPROX_PERCENTILE(next_prev_salary, ARRAY(0.25, 0.5, 0.75)) AS next_prev_salary_quartiles,
        SUM(CASE WHEN job_title = 'Intern' THEN event_seq ELSE 0 END) AS intern_event_seq,
        GROUPING(CASE WHEN prev_salary >= 138000 THEN 'LARGE' ELSE 'SMALL' END) AS gid
    FROM with_windows
    GROUP BY ROLLUP (CASE WHEN prev_salary >= 138000 THEN 'LARGE' ELSE 'SMALL' END, job_title)
)
SELECT
    prev_salary_tier,
    job_title,
    num_levels,
    l3_count,
    intern_event_seq,
    gid
FROM summary
