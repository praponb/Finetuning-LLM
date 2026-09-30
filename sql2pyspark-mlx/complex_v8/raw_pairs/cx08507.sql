WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        ABS(score - 38.5) AS score_distance,
        CASE WHEN score >= 80.2 THEN 'HIGH' WHEN score >= 20.2 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE term IN ('2024T1', '2025T2')
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.score_distance,
        s.grade_status_ref,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status
    FROM scoped_enrollments s
    JOIN enrollments e ON e.student_id = s.student_id
),
enriched_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.score_distance,
        s.grade_status_ref,
        s.enrollment_id,
        s.enrollment_course_id,
        s.enrollment_grade_status,
        c.credits AS course_credits,
        c.level AS course_level,
        c.dept AS course_dept
    FROM line_level s
    JOIN courses c ON c.id = s.enrollment_course_id
    WHERE c.dept = 'Math'
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    score_distance,
    grade_status_ref,
    enrollment_id,
    enrollment_course_id,
    enrollment_grade_status,
    course_credits,
    course_level,
    course_dept
FROM enriched_course
ORDER BY grade_status_ref NULLS LAST, enrollment_grade_status
