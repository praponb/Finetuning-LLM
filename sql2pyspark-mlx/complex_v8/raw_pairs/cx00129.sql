WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        ABS(score - 30.0) AS score_distance
    FROM enrollments
    WHERE course_id >= 5
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MIN(score) AS min_enrollment_score,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM prepared_enrollments s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status IS NOT NULL) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at
    FROM with_counts s
),
latest AS (
    -- Keep the latest row per grade_status
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        score_bucket,
        score_distance,
        min_enrollment_score,
        enrollment_count,
        enrollment_grade_status_count,
        first_enrollment_enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY first_enrollment_enrolled_at DESC, id DESC) AS recency_rank
        FROM profiled
    ) ranked
    WHERE recency_rank = 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        score_bucket,
        score_distance,
        enrollment_count,
        enrollment_grade_status_count,
        first_enrollment_enrolled_at,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        ROUND(AVG(enrollment_count) OVER (PARTITION BY student_id), 2) AS student_id_avg_enrollment_count,
        ROUND(AVG(enrollment_count) OVER (ORDER BY score_distance DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS enrollment_count_avg_2
    FROM latest
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    score_distance,
    enrollment_count,
    enrollment_grade_status_count,
    rows_in_student_id,
    student_id_avg_enrollment_count,
    enrollment_count_avg_2,
    COALESCE(enrollment_count_avg_2, 0.0) AS enrollment_count_avg_2_filled,
    LENGTH(TRIM(term)) AS term_len
FROM with_trends
ORDER BY enrollment_count NULLS LAST
