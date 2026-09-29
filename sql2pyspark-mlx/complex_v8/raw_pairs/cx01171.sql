WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-02-06 10:00:00'
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, ROUND(AVG(score), 2) AS avg_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.avg_enrollment_score
    FROM filtered_enrollments s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        FLOOR(avg_enrollment_score / 25) * 25 AS avg_enrollment_score_bucket
    FROM with_counts
    WHERE avg_enrollment_score BETWEEN 15.0 AND 65.0
),
with_parent_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.term_or_unknown,
        s.avg_enrollment_score,
        s.enrolled_at_label,
        s.avg_enrollment_score_bucket,
        st.enrolled_on AS student_enrolled_on,
        st.is_international AS student_is_international,
        c.course_code
    FROM scored s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    term_or_unknown,
    avg_enrollment_score,
    enrolled_at_label,
    student_enrolled_on,
    student_is_international,
    course_code,
    DATEDIFF(DATE '2026-01-01', student_enrolled_on) AS days_since_student_enrolled_on
FROM with_parent_student
