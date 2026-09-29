WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.days_since_enrolled_at,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.score AS enrollment_score
    FROM cleaned_enrollments s
    JOIN enrollments e ON e.course_id = s.course_id AND e.student_id IS NOT NULL
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.days_since_enrolled_at,
        s.enrollment_id,
        s.enrollment_student_id,
        s.enrollment_score,
        c.level AS course_level,
        c.dept AS course_dept,
        c.credits AS course_credits
    FROM detail_rows s
    LEFT JOIN courses c ON c.id = s.course_id
)
SELECT
    id,
    student_id,
    course_id,
    term,
    days_since_enrolled_at,
    enrollment_id,
    enrollment_student_id,
    course_level,
    CONCAT(course_level, '#', CAST(course_id AS STRING)) AS course_level_ref,
    ROUND(enrollment_score / NULLIF(days_since_enrolled_at, 0), 3) AS enrollment_score_per_days_since_enrolled_at
FROM joined_course
WHERE enrollment_student_id <= 9
ORDER BY id DESC NULLS FIRST, days_since_enrolled_at
