WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(budget) OVER w_ordered prev_budget,
        ROW_NUMBER() OVER w_ordered row_seq
    FROM departments
    WHERE dept_name IN ('Marketing', 'Finance')
    WINDOW w_ordered AS (PARTITION BY location ORDER BY budget, id)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.job_title NOT IN ('Intern', 'Analyst')
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location,
        budget,
        prev_budget,
        SUM(row_seq) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_row_seq,
        SUM(row_seq) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_row_seq,
        SUM(prev_budget) OVER w_hist location_sum_prev_budget
    FROM kept
    WINDOW w_hist AS (PARTITION BY location)
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.level,
        top1.job_title
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.level, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        dept_name,
        prev_budget,
        running_row_seq,
        cumulative_row_seq,
        location_sum_prev_budget,
        level,
        job_title
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST, id) AS rank_in_group
        FROM with_latest
    ) t
    WHERE rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_row_seq >= 43 THEN 'LARGE' ELSE 'SMALL' END AS running_row_seq_tier,
    CASE WHEN cumulative_row_seq >= 41 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_row_seq_tier,
    level,
    COUNT(DISTINCT level) AS distinct_levels,
    COUNT(DISTINCT job_title) AS distinct_job_titles,
    COUNT(*) FILTER (WHERE dept_name = 'Legal') AS legal_rows,
    SUM(CASE WHEN job_title = 'Director' THEN prev_budget ELSE 0 END) AS director_prev_budget,
    SUM(running_row_seq) FILTER (WHERE level = 'L5') AS l5_total_running_row_seq
FROM deduped
GROUP BY CASE WHEN running_row_seq >= 43 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN cumulative_row_seq >= 41 THEN 'LARGE' ELSE 'SMALL' END, level
