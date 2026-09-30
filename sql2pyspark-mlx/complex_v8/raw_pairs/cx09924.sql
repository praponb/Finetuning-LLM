WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2025T1', '2024T1') THEN 'group_a' WHEN term = '2024T2' THEN 'group_b' ELSE 'other' END AS term_group,
        CASE WHEN term IN ('2024T1', '2025T2') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group_2,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
    FROM enrollments
    WHERE (score >= 42.4 OR MONTH(enrolled_at) IN (1, 3))
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM scoped_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id IS NOT NULL
        )
      AND s.grade_status = 'FAIL'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.term_group,
        s.grade_status_prefix,
        c.credits AS course_credits,
        c.dept AS course_dept,
        c.course_code
    FROM qualified s
    LEFT JOIN courses c ON c.id = s.course_id
),
term_group_stats AS (
    -- Avg course_credits per term_group
    SELECT term_group, AVG(course_credits) AS avg_course_credits
    FROM with_parent
    GROUP BY term_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_course_credits,
        ROUND(s.course_credits - g.avg_course_credits, 2) AS course_credits_minus_avg
    FROM with_parent s
    JOIN term_group_stats g ON g.term_group = s.term_group
    WHERE s.course_credits > g.avg_course_credits
)
-- Subtotals with GROUPING SETS
SELECT
    course_credits,
    student_id,
    GROUPING(course_credits) AS is_total_course_credits,
    SUM(CASE WHEN course_dept NOT IN ('Art', 'Math') THEN course_credits ELSE 0 END) AS course_credits_course_dept_not_in_art_math,
    ROUND(STDDEV(course_credits), 2) AS stddev_course_credits
FROM compared
GROUP BY GROUPING SETS ((course_credits, student_id), (course_credits), (student_id))
ORDER BY course_credits NULLS LAST, student_id NULLS LAST
