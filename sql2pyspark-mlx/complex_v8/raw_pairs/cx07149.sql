WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 54.2) AS score_distance,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE score >= 11.3
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.enrolled_on AS student_enrolled_on,
        st.major AS student_major,
        st.cohort AS student_cohort
    FROM prepared s
    JOIN students st ON st.id = s.student_id
    WHERE st.enrolled_on < DATE '2025-03-20'
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(CASE WHEN term = '2025T2' THEN 1 END) AS enrollment_term_count
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count
    FROM enriched s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        score_distance,
        term_or_unknown,
        student_enrolled_on,
        student_major,
        student_cohort,
        DATEDIFF(DATE '2025-12-31', student_enrolled_on) AS days_since_student_enrolled_on,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM with_counts
    WHERE score_distance < 80.0
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    term_or_unknown,
    student_enrolled_on,
    student_cohort,
    days_since_student_enrolled_on,
    term_ref,
    grade_status_or_unknown,
    SUBSTRING(TRIM(grade_status_or_unknown), 1, 2) AS grade_status_or_unknown_prefix
FROM annotated
ORDER BY student_cohort
