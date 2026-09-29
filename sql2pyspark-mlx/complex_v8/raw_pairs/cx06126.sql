WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at_2,
        CASE WHEN grade_status IN ('INCOMPLETE', 'PASS') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE score > 10.8
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.days_since_enrolled_at,
        s.days_since_enrolled_at_2,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at
    FROM filtered s
    JOIN enrollments e ON e.student_id = s.student_id AND e.score < 79.2
)
SELECT
    student_id,
    enrollment_grade_status,
    SUM(CASE WHEN enrollment_grade_status = 'WITHDRAWN' THEN score ELSE 0 END) AS score_enrollment_grade_status_with,
    SUM(CASE WHEN enrollment_grade_status IS NOT NULL THEN 1 ELSE 0 END) AS n_enrollment_grade_status_is_n
FROM line_level_enrollments
GROUP BY GROUPING SETS ((student_id, enrollment_grade_status), (student_id), (enrollment_grade_status), ())
