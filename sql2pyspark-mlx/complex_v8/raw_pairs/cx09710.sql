WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref_2
    FROM enrollments
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM scoped_enrollments s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
            WHERE e.course_id <= 10
        )
),
with_parent AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.term_ref,
        s.term_ref_2,
        c.course_code
    FROM filtered s
    LEFT JOIN courses c ON c.id = s.course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS max_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM with_parent s
)
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    course_id,
    student_id,
    SUM(max_enrollment_score) AS total_max_enrollment_score,
    COUNT(*) AS row_count,
    MIN(enrollment_count) AS min_enrollment_count
FROM with_counts
GROUP BY CUBE(grade_status, course_id, student_id)
ORDER BY grade_status NULLS LAST, course_id NULLS LAST, student_id NULLS LAST
