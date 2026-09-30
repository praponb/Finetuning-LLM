WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-03-08 11:00:00'
      AND grade_status <> 'INCOMPLETE'
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        ROUND(AVG(score), 2) AS avg_enrollment_score,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.avg_enrollment_score,
        e.last_enrollment_enrolled_at,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM cleaned_enrollments s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
SELECT id, measure, value
FROM (
    SELECT id, score, avg_enrollment_score
    FROM summarized_rows
) src
UNPIVOT (
    value FOR measure IN (score, avg_enrollment_score)
) unp
