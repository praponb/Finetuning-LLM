WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month
    FROM enrollments
    WHERE (course_id IS NOT NULL OR student_id IS NOT NULL)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(score / NULLIF(score_bucket, 0), 3) AS score_per_score_bucket,
        COALESCE(score_bucket, 0) AS score_bucket_filled,
        UPPER(TRIM(grade_status)) AS grade_status_clean
    FROM base
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM tagged s
)
-- Subtotals with GROUPING SETS
SELECT
    term,
    enrollment_count,
    grade_status,
    COUNT(*) FILTER (WHERE term IS NOT NULL) AS n_term_is_not_null
FROM with_stats
GROUP BY GROUPING SETS ((term, enrollment_count, grade_status), (term), ())
ORDER BY term NULLS LAST, enrollment_count NULLS LAST, grade_status NULLS LAST
