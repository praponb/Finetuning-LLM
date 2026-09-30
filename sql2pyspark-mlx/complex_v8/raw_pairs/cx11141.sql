WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) AS enrollment_count,
        COUNT(*) AS enrollment_count_2,
        SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2,
        e.total_enrollment_score
    FROM base_enrollments s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY term, enrollment_count_2)) AS distinct_grade_status_in_term_enrollment_count_2,
        ROUND(total_enrollment_score - AVG(total_enrollment_score) OVER (PARTITION BY term, enrollment_count_2), 2) AS total_enrollment_score_vs_term_enrollment_count_,
        NTILE(5) OVER (PARTITION BY enrollment_count_2, enrollment_count ORDER BY score, id) AS score_quintile
    FROM with_counts
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term IN ('2025T2', '2025T1', '2024T2')
              AND e.score <= 78.8
        )
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.days_since_enrolled_at,
        s.enrollment_count,
        s.total_enrollment_score,
        s.total_enrollment_score_vs_term_enrollment_count_,
        s.score_quintile,
        st.enrolled_on AS student_enrolled_on,
        c.level AS course_level,
        c.course_code
    FROM screened s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.total_enrollment_score,
        s.score_quintile,
        s.course_level,
        s.course_code,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id,
        e.term AS enrollment_term
    FROM with_parent_student s
    JOIN enrollments e ON e.course_id = s.course_id
),
latest_per_key AS (
    -- Keep the earliest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        total_enrollment_score,
        score_quintile,
        course_level,
        course_code,
        enrollment_id,
        enrollment_enrolled_at,
        enrollment_student_id,
        enrollment_term
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrollment_enrolled_at, id, enrollment_id) AS row_num
        FROM detail_rows_enrollments
    ) ranked
    WHERE row_num = 1
)
SELECT *
FROM (
    SELECT enrollment_term, enrollment_id, grade_status, total_enrollment_score
    FROM latest_per_key
) src
PIVOT (
    MIN(total_enrollment_score)
    FOR grade_status IN ('PASS', 'FAIL', 'INCOMPLETE')
)
ORDER BY enrollment_term, enrollment_id
