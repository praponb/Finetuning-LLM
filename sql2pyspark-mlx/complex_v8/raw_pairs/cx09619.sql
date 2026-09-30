WITH prepared_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE gpa >= 2.7
      AND NOT is_international
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        SUM(score) AS total_enrollment_score,
        COUNT(*) FILTER (WHERE grade_status = 'PASS') AS enrollment_grade_status_count,
        MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.total_enrollment_score,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count,
        e.min_enrollment_score
    FROM prepared_students s
    LEFT JOIN enrollment_stats e ON e.student_id = s.id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        total_enrollment_score,
        enrollment_grade_status_count,
        CONCAT_WS(': ', major, student_name) AS major_student_name_label,
        GREATEST(gpa, min_enrollment_score) AS greatest_gpa_min_enrollment_score
    FROM with_stats
)
SELECT DISTINCT greatest_gpa_min_enrollment_score
FROM tagged
ORDER BY greatest_gpa_min_enrollment_score
