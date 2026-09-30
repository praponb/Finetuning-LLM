WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE grade_status = 'FAIL'
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM cleaned_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.enrolled_at < TIMESTAMP '2025-03-02 09:00:00'
        )
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at
    FROM without_matches s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
enrollment_stats_2 AS (
    -- Per-student enrollments metrics
    SELECT student_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM profiled s
    LEFT JOIN enrollment_stats_2 e ON e.student_id = s.student_id
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    first_enrollment_enrolled_at,
    ROUND(score * 0.9, 2) AS score_adj,
    LOWER(TRIM(grade_status_ref)) AS grade_status_ref_clean
FROM summarized_rows
ORDER BY grade_status NULLS LAST
