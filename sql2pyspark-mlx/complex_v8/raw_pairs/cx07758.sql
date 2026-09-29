WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref,
        CASE WHEN score >= 57.8 THEN 'HIGH' WHEN score >= 13.9 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        enrolled_at + INTERVAL 15 MINUTES AS enrolled_at_plus_15m
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
      AND student_id >= 13
),
joined AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.term_ref,
        s.enrolled_at_plus_15m,
        c.credits AS course_credits,
        c.dept AS course_dept,
        c.course_code
    FROM filtered s
    JOIN courses c ON c.id = s.course_id
)
-- Subtotals with CUBE
SELECT
    course_credits,
    course_id,
    GROUPING_ID() AS grouping_level,
    SUM(course_credits) AS total_course_credits
FROM joined
GROUP BY CUBE(course_credits, course_id)
ORDER BY course_credits NULLS LAST, course_id NULLS LAST
