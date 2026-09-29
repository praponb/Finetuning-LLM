WITH valid_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        level,
        CONCAT_WS('|', course_code, level) AS course_code_level_label,
        SUBSTRING(TRIM(level), 1, 3) AS level_prefix,
        LENGTH(TRIM(dept)) AS dept_len
    FROM courses
    WHERE (level IS NOT NULL OR credits > 2)
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM valid_courses s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.score > 5.9
              AND TO_DATE(e.enrolled_at) < DATE '2025-03-14'
        )
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.dept,
        s.course_code_level_label,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id
    FROM without_matches s
    JOIN enrollments e ON e.course_id = s.id AND e.student_id IS NOT NULL
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.enrollment_student_id) AS avg_enrollment_score,
        (SELECT COUNT(CASE WHEN e.term <> '2025T1' THEN 1 END) FROM enrollments e WHERE e.student_id = s.enrollment_student_id) AS enrollment_term_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.enrollment_student_id) AS enrollment_count
    FROM line_level_enrollments s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrollment_id, enrollment_student_id, dept, avg_enrollment_score
    FROM with_counts
) src
PIVOT (
    COUNT(avg_enrollment_score) AS count_v, SUM(avg_enrollment_score) AS sum_v
    FOR dept IN ('Art', 'CS', 'Biz')
)
