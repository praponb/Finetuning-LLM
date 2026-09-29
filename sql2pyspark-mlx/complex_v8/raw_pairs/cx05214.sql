WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 15:00:00'
),
with_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_label,
        c.course_code,
        st.gpa AS student_gpa
    FROM valid s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        SUM(score) AS total_enrollment_score,
        COUNT(*) FILTER (WHERE term = '2024T1') AS enrollment_term_count,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.total_enrollment_score,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM with_details_course s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    enrolled_at_label,
    course_code,
    student_gpa,
    total_enrollment_score,
    enrollment_term_count,
    enrollment_count,
    LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
FROM summarized_rows
WHERE student_gpa < 3.2
ORDER BY enrollment_count DESC NULLS FIRST, id
LIMIT 5
