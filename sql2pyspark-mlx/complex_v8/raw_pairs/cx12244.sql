WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        YEAR(enrolled_at) AS enrolled_at_year,
        CASE WHEN term IN ('2024T2', '2024T1') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 7 AND 9
),
unique_rows AS (
    -- Keep the latest row per course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at_year,
        term_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY score DESC, id DESC) AS rn
        FROM cleaned_enrollments
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at_year,
        s.term_group,
        c.level AS course_level,
        c.dept AS course_dept,
        c.credits AS course_credits
    FROM unique_rows s
    JOIN courses c ON c.id = s.course_id
)
-- Subtotals with ROLLUP
SELECT
    course_credits,
    course_id,
    grade_status,
    GROUPING_ID() AS grouping_level,
    ROUND(AVG(score), 2) AS avg_score,
    ARRAY_SORT(COLLECT_SET(course_dept)) AS course_dept_list
FROM enriched
GROUP BY ROLLUP(course_credits, course_id, grade_status)
ORDER BY course_credits NULLS LAST, course_id NULLS LAST, grade_status NULLS LAST
