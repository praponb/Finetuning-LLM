WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE course_id >= 10
      AND score <= 73.7
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MIN(score) AS min_enrollment_score,
        COUNT(*) FILTER (WHERE grade_status = 'WITHDRAWN') AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM filtered_enrollments s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
with_details_course AS (
    -- Attach course attributes
    SELECT s.*, c.dept AS course_dept, c.course_code
    FROM summarized_rows s
    JOIN courses c ON c.id = s.course_id
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    grade_status_or_unknown,
    enrollment_grade_status_count,
    course_code,
    FLOOR(score_bucket / 25) * 25 AS score_bucket_bucket,
    ABS(score - 32.7) AS score_distance
FROM with_details_course
WHERE LENGTH(TRIM(course_code)) >= 6
ORDER BY score_bucket_bucket DESC, enrolled_at
