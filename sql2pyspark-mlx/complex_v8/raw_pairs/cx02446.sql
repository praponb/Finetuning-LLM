WITH filtered AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        UPPER(TRIM(major)) AS major_clean
    FROM students
    WHERE major IN ('Art', 'Math', 'CS')
      AND is_international = FALSE
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.cohort,
        s.enrolled_on,
        s.is_international,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term,
        e.score AS enrollment_score
    FROM filtered s
    JOIN enrollments e ON e.student_id = s.id
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM exploded_detail_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.term NOT IN ('2024T2', '2024T1')
        )
      AND s.enrolled_on < DATE '2025-06-27'
),
qualified_2 AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM qualified s
    WHERE s.id IN (
            SELECT e.student_id
            FROM enrollments e
        )
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        cohort,
        enrolled_on,
        is_international,
        enrollment_id,
        enrollment_grade_status,
        enrollment_enrolled_at,
        enrollment_term,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(enrollment_enrolled_at), enrollment_grade_status ORDER BY enrolled_on, id, enrollment_id), 3) AS enrolled_on_percent_rank,
        LAST_VALUE(enrollment_score) IGNORE NULLS OVER (ORDER BY enrolled_on DESC, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrollment_score,
        LAG(enrolled_on, 2) OVER w_all_by_enrolled_on AS prev2_enrolled_on,
        LAST_VALUE(enrollment_enrolled_at) IGNORE NULLS OVER (PARTITION BY MONTH(enrollment_enrolled_at), enrollment_grade_status ORDER BY enrolled_on, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrollment_enrolled_at,
        enrollment_score - LAG(enrollment_score) OVER (PARTITION BY MONTH(enrollment_enrolled_at), enrollment_grade_status ORDER BY enrolled_on, id, enrollment_id) AS enrollment_score_change,
        SIZE(COLLECT_SET(cohort) OVER w_is_international_enrollment) AS distinct_cohort_in_is_international_enrollment_i
    FROM qualified_2
    WINDOW w_all_by_enrolled_on AS (ORDER BY enrolled_on DESC, id, enrollment_id), w_is_international_enrollment AS (PARTITION BY is_international, enrollment_id)
)
SELECT
    id,
    cohort,
    enrolled_on,
    is_international,
    enrollment_id,
    enrollment_grade_status,
    enrollment_enrolled_at,
    enrolled_on_percent_rank,
    prev2_enrolled_on,
    last_known_enrollment_enrolled_at,
    enrollment_score_change,
    distinct_cohort_in_is_international_enrollment_i,
    SUBSTRING(TRIM(enrollment_grade_status), 1, 4) AS enrollment_grade_status_prefix,
    YEAR(enrolled_on) AS enrolled_on_year
FROM with_trends
WHERE prev2_enrolled_on >= DATE '2024-04-04'
ORDER BY enrollment_id DESC, distinct_cohort_in_is_international_enrollment_i
