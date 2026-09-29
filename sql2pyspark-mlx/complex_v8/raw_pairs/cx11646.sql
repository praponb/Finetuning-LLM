WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-02-06'
      AND score < 62.5
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        DATEDIFF(enrolled_at_date, LAG(enrolled_at_date) OVER (ORDER BY enrolled_at_date DESC, id)) AS days_since_prev_enrolled_at_date,
        SUM(score) OVER (ORDER BY enrolled_at_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_sum_6_prior,
        SUM(score) OVER (PARTITION BY term ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_sum_3
    FROM base
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status_prefix,
        s.term_prefix,
        s.enrolled_at_date,
        s.rows_in_student_id,
        s.days_since_prev_enrolled_at_date,
        s.score_sum_6_prior,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status
    FROM with_trends s
    JOIN enrollments e ON e.course_id = s.course_id AND e.enrolled_at >= TIMESTAMP '2025-02-06 10:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(enrolled_at_date) OVER (ORDER BY enrolled_at_date DESC, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_date,
        NTILE(4) OVER w_enrollment_enrolled_at_month AS days_since_prev_enrolled_at_date_quartile,
        NTH_VALUE(rows_in_student_id, 2) OVER (PARTITION BY MONTH(enrollment_enrolled_at), enrollment_id ORDER BY days_since_prev_enrolled_at_date, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_student_id,
        DENSE_RANK() OVER w_all_by_enrolled_at_date_ties AS enrolled_at_date_dense_rank,
        SIZE(COLLECT_SET(enrollment_student_id) OVER (PARTITION BY student_id, enrollment_grade_status)) AS distinct_enrollment_student_id_in_student_id_enr,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at_date DESC), 3) AS enrolled_at_date_cume_dist
    FROM line_level
    WINDOW w_enrollment_enrolled_at_month AS (PARTITION BY MONTH(enrollment_enrolled_at), enrollment_id ORDER BY days_since_prev_enrolled_at_date, id, enrollment_id), w_all_by_enrolled_at_date_ties AS (ORDER BY enrolled_at_date DESC)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status_prefix,
    term_prefix,
    enrolled_at_date,
    rows_in_student_id,
    days_since_prev_enrolled_at_date,
    score_sum_6_prior,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_student_id,
    enrollment_grade_status,
    first_enrolled_at_date,
    days_since_prev_enrolled_at_date_quartile,
    second_rows_in_student_id,
    distinct_enrollment_student_id_in_student_id_enr,
    enrolled_at_date_cume_dist,
    CONCAT_WS('-', term_prefix, enrollment_grade_status) AS term_prefix_enrollment_grade_status_label
FROM with_windows
ORDER BY term_prefix DESC, course_id, id, enrollment_id
LIMIT 5
