WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score > 35.8
      AND grade_status <> 'WITHDRAWN'
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM prepared_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.enrolled_at < TIMESTAMP '2025-03-08 10:00:00'
        )
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(score) AS min_enrollment_score,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        MIN(score) AS min_enrollment_score_2
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        e.first_enrollment_enrolled_at,
        e.min_enrollment_score_2
    FROM without_matches s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.term = '2024T1' THEN 1 END) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_term_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM with_counts s
)
-- Subtotals with GROUPING SETS
SELECT
    term,
    enrollment_count,
    SUM(CASE WHEN grade_status NOT IN ('WITHDRAWN', 'PASS') THEN enrollment_term_count ELSE 0 END) AS enrollment_term_count_grade_status_not_in_withdr,
    MAX(enrolled_at) AS last_enrolled_at
FROM profiled
GROUP BY GROUPING SETS ((term, enrollment_count), (term))
