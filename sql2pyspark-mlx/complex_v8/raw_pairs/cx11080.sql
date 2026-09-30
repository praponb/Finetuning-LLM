WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        FLOOR(score / 25) * 25 AS score_bucket,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE course_id >= 3
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        TO_DATE(enrolled_at) AS enrolled_at_date_2,
        ABS(score_bucket - 35) AS score_bucket_distance,
        LENGTH(TRIM(grade_status_or_unknown)) AS grade_status_or_unknown_len
    FROM base_enrollments
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM enriched s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    grade_status,
    enrolled_at,
    grade_status_or_unknown,
    score_bucket,
    enrolled_at_date,
    enrolled_at_date_2,
    score_bucket_distance,
    grade_status_or_unknown_len,
    DATE_FORMAT(enrolled_at_date_2, 'yyyy-MM') AS enrolled_at_date_2_label
FROM with_counts
WHERE score_bucket >= 20
ORDER BY enrolled_at_date_2
