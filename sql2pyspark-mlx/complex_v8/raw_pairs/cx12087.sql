WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status IN ('FAIL', 'INCOMPLETE')) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM cleaned s
),
student_id_stats AS (
    -- Max enrollment_grade_status_count per student_id
    SELECT student_id, MAX(enrollment_grade_status_count) AS max_enrollment_grade_status_count
    FROM summarized_rows
    GROUP BY student_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_enrollment_grade_status_count,
        ROUND(s.enrollment_grade_status_count - g.max_enrollment_grade_status_count, 2) AS enrollment_grade_status_count_minus_max
    FROM summarized_rows s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.enrollment_grade_status_count = g.max_enrollment_grade_status_count
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        grade_status,
        enrollment_count,
        course_id,
        ROUND(STDDEV(enrollment_count), 2) AS stddev_enrollment_count,
        SUM(enrollment_count) AS total_enrollment_count,
        COUNT(*) AS cnt
    FROM vs_group
    WHERE HOUR(enrolled_at) BETWEEN 10 AND 15
    GROUP BY 1, 2, 3
),
labeled AS (
    -- Derive helper columns
    SELECT
        grade_status,
        enrollment_count,
        course_id,
        total_enrollment_count,
        cnt,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM per_group
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        grade_status,
        enrollment_count,
        course_id,
        cnt,
        ROUND(AVG(enrollment_count) OVER (PARTITION BY grade_status_len ORDER BY total_enrollment_count DESC, grade_status, enrollment_count, course_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS enrollment_count_avg_3,
        ROUND(AVG(enrollment_count) OVER (PARTITION BY grade_status_len ORDER BY total_enrollment_count DESC, grade_status, enrollment_count, course_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS enrollment_count_avg_3_prior,
        LAG(total_enrollment_count) OVER (PARTITION BY grade_status_len ORDER BY cnt ASC, grade_status, enrollment_count, course_id) AS prev_total_enrollment_count
    FROM labeled
)
-- Final projection
SELECT
    grade_status,
    enrollment_count,
    course_id,
    enrollment_count_avg_3,
    enrollment_count_avg_3_prior,
    prev_total_enrollment_count
FROM windowed
ORDER BY enrollment_count DESC, enrollment_count_avg_3_prior
