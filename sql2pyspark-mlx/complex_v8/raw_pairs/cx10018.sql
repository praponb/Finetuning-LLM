WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label_2
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND score >= 10.1
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM valid_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status = 'WITHDRAWN'
        )
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at_dayofweek,
        grade_status_term_label,
        grade_status_term_label_2,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek_2,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek_3,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM without_matches
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at_dayofweek_2,
        s.enrolled_at_dayofweek_3,
        s.enrolled_at_date,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id
    FROM derived s
    JOIN enrollments e ON e.course_id = s.course_id
),
enrollment_term_stats AS (
    -- Median enrolled_at_dayofweek_3 per enrollment_term
    SELECT enrollment_term, PERCENTILE_APPROX(enrolled_at_dayofweek_3, 0.5) AS median_enrolled_at_dayofweek_3
    FROM exploded_detail
    GROUP BY enrollment_term
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_enrolled_at_dayofweek_3,
        ROUND(s.enrolled_at_dayofweek_3 - g.median_enrolled_at_dayofweek_3, 2) AS enrolled_at_dayofweek_3_minus_median
    FROM exploded_detail s
    JOIN enrollment_term_stats g ON g.enrollment_term = s.enrollment_term
    WHERE s.enrolled_at_dayofweek_3 >= g.median_enrolled_at_dayofweek_3
)
SELECT
    id,
    student_id,
    course_id,
    enrolled_at_dayofweek_2,
    enrolled_at_dayofweek_3,
    enrolled_at_date,
    enrollment_id,
    enrollment_term,
    enrollment_student_id,
    enrolled_at_dayofweek_3_minus_median,
    LENGTH(TRIM(enrollment_term)) AS enrollment_term_len
FROM outliers
ORDER BY enrolled_at_dayofweek_3_minus_median DESC
