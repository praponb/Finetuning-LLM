WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        CONCAT_WS('|', term, grade_status) AS term_grade_status_label,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE score < 79.3
      AND course_id IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        term,
        score,
        enrolled_at,
        term_grade_status_label,
        term_or_unknown,
        NTILE(3) OVER (PARTITION BY term_or_unknown, term ORDER BY score DESC, id) AS score_tercile,
        LAST_VALUE(score) OVER (PARTITION BY term_or_unknown, course_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score
    FROM cleaned_enrollments
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(term_grade_status_label), 1, 3) AS term_grade_status_label_prefix
    FROM ranked
    WHERE term_or_unknown <> '2024T2'
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.term_grade_status_label,
        s.score_tercile,
        s.term_grade_status_label_prefix,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM annotated s
    JOIN enrollments e ON e.student_id = s.student_id AND e.grade_status = 'INCOMPLETE'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.term_grade_status_label,
        s.term_grade_status_label_prefix,
        s.enrollment_id,
        s.enrollment_term,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at_2,
        e.grade_status AS enrollment_grade_status_2,
        c.level AS course_level
    FROM detail_rows_enrollments s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN courses c ON c.id = e.course_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        enrolled_at,
        enrollment_id,
        enrollment_term,
        enrollment_course_id,
        enrollment_enrolled_at_2,
        enrollment_grade_status_2,
        course_level,
        LAST_VALUE(enrollment_enrolled_at_2) IGNORE NULLS OVER (PARTITION BY enrollment_id, enrollment_term ORDER BY enrolled_at DESC, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrollment_enrolled_at_2,
        LAG(enrollment_enrolled_at_2) OVER (PARTITION BY enrollment_id, enrollment_term ORDER BY enrolled_at DESC, id, enrollment_id) AS prev_enrollment_enrolled_at_2,
        LAG(enrollment_enrolled_at_2, 2) OVER w_enrollment_id_enrollment_ter_2 AS prev2_enrollment_enrolled_at_2
    FROM with_details
    WINDOW w_enrollment_id_enrollment_ter_2 AS (PARTITION BY enrollment_id, enrollment_term ORDER BY enrolled_at DESC, id, enrollment_id)
)
SELECT
    id,
    student_id,
    enrolled_at,
    enrollment_id,
    enrollment_term,
    enrollment_course_id,
    last_known_enrollment_enrolled_at_2,
    prev2_enrollment_enrolled_at_2
FROM with_trends
ORDER BY enrolled_at DESC NULLS FIRST, enrollment_id
