WITH recent_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        COALESCE(cohort, 'UNKNOWN') AS cohort_or_unknown
    FROM students
    WHERE gpa > 1.7
      AND NOT is_international
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(score) AS min_enrollment_score,
        MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        e.first_enrollment_enrolled_at
    FROM recent_students s
    LEFT JOIN enrollment_stats e ON e.student_id = s.id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        DATEDIFF(enrolled_on, LAG(enrolled_on) OVER (PARTITION BY cohort_or_unknown, major ORDER BY min_enrollment_score DESC, id)) AS days_since_prev_enrolled_on,
        MAX(min_enrollment_score) OVER (PARTITION BY MONTH(first_enrollment_enrolled_at), cohort) AS first_enrollment_enrolled_at_month_cohort_max_mi,
        LAG(min_enrollment_score) OVER (PARTITION BY cohort_or_unknown, major ORDER BY min_enrollment_score DESC, id) AS prev_min_enrollment_score,
        ROUND(min_enrollment_score / NULLIF(SUM(min_enrollment_score) OVER w_first_enrollment_enrolled_at, 0), 4) AS min_enrollment_score_share_of_first_enrollment_e,
        FIRST_VALUE(enrolled_on) IGNORE NULLS OVER (PARTITION BY cohort_or_unknown, major ORDER BY min_enrollment_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_on
    FROM summarized_rows
    WINDOW w_first_enrollment_enrolled_at AS (PARTITION BY MONTH(first_enrollment_enrolled_at), cohort)
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.major,
        s.enrolled_on,
        s.min_enrollment_score,
        s.first_enrollment_enrolled_at,
        s.first_enrollment_enrolled_at_month_cohort_max_mi,
        s.min_enrollment_score_share_of_first_enrollment_e,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at
    FROM with_trends s
    JOIN enrollments e ON e.student_id = s.id AND TO_DATE(e.enrolled_at) < DATE '2025-02-18'
)
SELECT
    COALESCE(enrollment_grade_status, 'ALL') AS enrollment_grade_status,
    enrollment_id,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT major) AS distinct_majors,
    SUM(first_enrollment_enrolled_at_month_cohort_max_mi) AS total_first_enrollment_enrolled_at_month_cohort_
FROM detail_rows
GROUP BY ROLLUP(enrollment_grade_status, enrollment_id)
ORDER BY enrollment_grade_status NULLS LAST, enrollment_id NULLS LAST
