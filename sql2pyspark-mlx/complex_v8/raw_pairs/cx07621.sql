WITH base_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        UPPER(TRIM(level)) AS level_clean
    FROM courses
    WHERE dept = 'Biz'
),
scored AS (
    -- Add calculated columns
    SELECT *, COALESCE(dept, 'UNKNOWN') AS dept_or_unknown
    FROM base_courses
    WHERE LENGTH(TRIM(course_code)) >= 8
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(credits) OVER (PARTITION BY dept_or_unknown, dept) AS dept_or_unknown_dept_sum_credits,
        LEAD(credits) OVER (ORDER BY credits ASC, id) AS next_credits,
        credits - LAG(credits) OVER (ORDER BY credits ASC, id) AS credits_change,
        LAST_VALUE(credits) IGNORE NULLS OVER (ORDER BY credits ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credits,
        credits - LAG(credits) OVER (PARTITION BY dept ORDER BY credits DESC, id) AS credits_change_2
    FROM scored
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.grade_status = 'PASS'
        )
      AND s.level_clean IS NOT NULL
),
dept_or_unknown_stats AS (
    -- Avg credits_change per dept_or_unknown
    SELECT dept_or_unknown, AVG(credits_change) AS avg_credits_change
    FROM screened
    GROUP BY dept_or_unknown
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_credits_change,
        ROUND(s.credits_change - g.avg_credits_change, 2) AS credits_change_minus_avg
    FROM screened s
    JOIN dept_or_unknown_stats g ON g.dept_or_unknown = s.dept_or_unknown
    WHERE s.credits_change > g.avg_credits_change
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        course_code,
        credits,
        dept_or_unknown,
        dept_or_unknown_dept_sum_credits,
        next_credits,
        credits_change,
        last_known_credits,
        credits_change_minus_avg,
        LOWER(TRIM(dept_or_unknown)) AS dept_or_unknown_clean
    FROM compared
    WHERE last_known_credits > 2
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_code,
        credits,
        dept_or_unknown,
        dept_or_unknown_dept_sum_credits,
        next_credits,
        credits_change,
        last_known_credits,
        credits_change_minus_avg,
        FIRST_VALUE(credits_change_minus_avg) OVER (PARTITION BY last_known_credits ORDER BY last_known_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_credits_change_minus_avg,
        ROUND(next_credits - AVG(next_credits) OVER (PARTITION BY next_credits, credits), 2) AS next_credits_vs_next_credits_credits_avg,
        RANK() OVER w_credits_credits_change_by_de AS dept_or_unknown_dept_sum_credits_rank
    FROM labeled
    WINDOW w_credits_credits_change_by_de AS (PARTITION BY credits, credits_change ORDER BY dept_or_unknown_dept_sum_credits ASC, id)
)
SELECT
    id,
    course_code,
    dept_or_unknown_dept_sum_credits,
    next_credits,
    credits_change,
    last_known_credits,
    credits_change_minus_avg,
    first_credits_change_minus_avg,
    next_credits_vs_next_credits_credits_avg,
    CASE WHEN dept_or_unknown IN ('CS', 'Art') THEN 'group_a' WHEN dept_or_unknown = 'Math' THEN 'group_b' ELSE 'other' END AS dept_or_unknown_group,
    ROUND(credits_change_minus_avg * 1.1, 2) AS credits_change_minus_avg_adj
FROM windowed_2
ORDER BY next_credits DESC
