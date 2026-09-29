WITH recent_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        CASE WHEN gpa >= 3.2 THEN 'A' WHEN gpa >= 2.5 THEN 'B' ELSE 'C' END AS gpa_tier
    FROM students
    WHERE LOWER(student_name) LIKE '%ar%'
      AND major IS NOT NULL
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.course_id AS enrollment_course_id
    FROM recent_students s
    JOIN enrollments e ON e.student_id = s.id
),
enrollment_course_id_stats AS (
    -- Avg enrollment_score per enrollment_course_id
    SELECT enrollment_course_id, AVG(enrollment_score) AS avg_enrollment_score
    FROM exploded_detail
    GROUP BY enrollment_course_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_enrollment_score,
        ROUND(s.enrollment_score - g.avg_enrollment_score, 2) AS enrollment_score_minus_avg
    FROM exploded_detail s
    JOIN enrollment_course_id_stats g ON g.enrollment_course_id = s.enrollment_course_id
    WHERE s.enrollment_score > g.avg_enrollment_score
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM compared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.enrollment_course_id
              AND e.enrolled_at >= TIMESTAMP '2025-01-31 14:00:00'
        )
)
-- Subtotals with CUBE
SELECT
    enrollment_course_id,
    COALESCE(enrollment_grade_status, 'ALL') AS enrollment_grade_status,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE enrollment_grade_status IN ('WITHDRAWN', 'INCOMPLETE')) AS n_enrollment_grade_status_in_w
FROM without_matches
GROUP BY CUBE(enrollment_course_id, enrollment_grade_status)
