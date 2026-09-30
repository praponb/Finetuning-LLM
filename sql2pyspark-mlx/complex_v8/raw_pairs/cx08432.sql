WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        ROUND(score * 0.25, 2) AS score_adj,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE course_id <= 4
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count_2,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count_3
    FROM valid_enrollments s
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(grade_status_len, score_bucket) AS least_grade_status_len_score_bucket,
        GREATEST(enrollment_count, score_bucket) AS greatest_enrollment_count_score_bucket
    FROM summarized_rows
)
-- Subtotals with ROLLUP
SELECT
    course_id,
    enrollment_count_3,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(grade_status_len) / NULLIF(SUM(greatest_enrollment_count_score_bucket), 0), 3) AS grade_status_len_to_greatest_enrollment_count_sc
FROM tagged
GROUP BY ROLLUP(course_id, enrollment_count_3)
