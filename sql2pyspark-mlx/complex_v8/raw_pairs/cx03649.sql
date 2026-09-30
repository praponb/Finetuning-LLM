WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-12 12:00:00'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM prepared_enrollments
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.term_prefix,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score
    FROM tagged s
    JOIN enrollments e ON e.course_id = s.course_id
),
course_id_stats AS (
    -- Median enrollment_score per course_id
    SELECT course_id, PERCENTILE_APPROX(enrollment_score, 0.5) AS median_enrollment_score
    FROM line_level_enrollments
    GROUP BY course_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_enrollment_score,
        ROUND(s.enrollment_score - g.median_enrollment_score, 2) AS enrollment_score_minus_median
    FROM line_level_enrollments s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.enrollment_score >= g.median_enrollment_score
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM outliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id IS NOT NULL
              AND HOUR(e.enrolled_at) BETWEEN 10 AND 13
        )
)
SELECT
    id,
    course_id,
    term,
    term_prefix,
    enrollment_id,
    enrollment_student_id,
    median_enrollment_score,
    SUBSTRING(TRIM(term_prefix), 1, 2) AS term_prefix_prefix,
    ROUND(median_enrollment_score * 0.9, 2) AS median_enrollment_score_adj
FROM without_matches
ORDER BY id DESC, median_enrollment_score_adj
