WITH cleaned AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE major IS NOT NULL
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.cohort,
        s.is_international,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id
    FROM cleaned s
    JOIN enrollments e ON e.student_id = s.id
),
combined_details_enrollment AS (
    -- Attach enrollment attributes
    SELECT
        s.id,
        s.student_name,
        s.cohort,
        s.is_international,
        s.enrollment_id,
        s.enrollment_grade_status,
        s.enrollment_enrolled_at,
        s.enrollment_score,
        s.enrollment_course_id,
        e.term AS enrollment_term
    FROM detail_rows_enrollments s
    JOIN enrollments e ON e.id = s.enrollment_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        cohort,
        is_international,
        enrollment_id,
        enrollment_enrolled_at,
        enrollment_score,
        enrollment_course_id,
        ROUND(enrollment_score - AVG(enrollment_score) OVER (PARTITION BY enrollment_grade_status), 2) AS enrollment_score_vs_enrollment_grade_status_avg,
        ROUND(AVG(enrollment_score) OVER (PARTITION BY enrollment_grade_status), 2) AS enrollment_grade_status_avg_enrollment_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrollment_score), 3) AS enrollment_score_percent_rank
    FROM combined_details_enrollment
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.is_international,
        s.enrollment_id,
        s.enrollment_enrolled_at,
        s.enrollment_course_id,
        s.enrollment_grade_status_avg_enrollment_score,
        e.course_id AS enrollment_course_id_2,
        e.term AS enrollment_term
    FROM sequenced s
    JOIN enrollments e ON e.id = s.enrollment_id AND e.grade_status = 'PASS'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(enrollment_enrolled_at, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', enrollment_enrolled_at) ORDER BY enrollment_enrolled_at DESC, id, enrollment_id) AS next3_enrollment_enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrollment_enrolled_at) - UNIX_TIMESTAMP(LAG(enrollment_enrolled_at) OVER (PARTITION BY DATE_TRUNC('MONTH', enrollment_enrolled_at) ORDER BY enrollment_enrolled_at DESC, id, enrollment_id))) / 60, 1) AS minutes_since_prev_enrollment_enrolled_at
    FROM joined
)
-- Final projection
SELECT
    id,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_grade_status_avg_enrollment_score,
    enrollment_course_id_2,
    next3_enrollment_enrolled_at,
    IF(is_international, 'yes', 'no') AS is_international_label,
    DATE_TRUNC('YEAR', enrollment_enrolled_at) AS enrollment_enrolled_at_year
FROM windowed
WHERE enrollment_course_id >= 4
ORDER BY enrollment_enrolled_at_year, next3_enrollment_enrolled_at DESC, id, enrollment_id
LIMIT 20
