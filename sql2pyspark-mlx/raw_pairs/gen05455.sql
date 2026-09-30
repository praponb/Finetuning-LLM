WITH with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.job_title,
        x.level
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN level IN ('L2', 'L3') THEN 'Core' ELSE 'Other' END AS level_group
    FROM with_last
    WHERE dept_name IS NOT NULL
      AND location IN ('New York', 'Bangkok', 'Tokyo')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY budget DESC NULLS LAST, id ASC) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_name,
        budget,
        job_title,
        level,
        level_group,
        dedup_rank,
        ROUND(budget, 2) AS budget_rounded
    FROM deduped
    WHERE job_title IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(budget) OVER (PARTITION BY job_title, level ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS budget_avg_5,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY budget DESC NULLS LAST) AS budget_cume_dist,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget ASC NULLS FIRST) AS budget_cume_dist_2
    FROM base
    WHERE job_title = 'Manager'
)
-- Grouped metrics
SELECT
    dept_name,
    level,
    SUM(budget) FILTER (WHERE job_title = 'Intern') AS intern_total_budget,
    ROUND(SUM(budget_cume_dist_2) / NULLIF(COUNT(CASE WHEN job_title = 'Analyst' THEN 1 END), 0), 2) AS budget_cume_dist_2_per_analyst,
    GROUPING(dept_name) AS gid
FROM scored
GROUP BY CUBE (dept_name, level)
HAVING SUM(budget) FILTER (WHERE job_title = 'Intern') < 1000000;
