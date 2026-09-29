WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        UPPER(TRIM(grade_status)) AS grade_status_clean,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM enrollments
    WHERE (student_id <= 7 OR term IN ('2024T2', '2025T2'))
),
enriched_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.grade_status_clean,
        s.enrolled_at_week,
        st.gpa AS student_gpa,
        st.enrolled_on AS student_enrolled_on,
        st.student_name
    FROM valid s
    LEFT JOIN students st ON st.id = s.student_id
),
joined_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status_clean,
        s.student_gpa,
        s.student_enrolled_on,
        s.student_name,
        c.course_code
    FROM enriched_student s
    LEFT JOIN courses c ON c.id = s.course_id AND c.credits > 2
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE term NOT IN ('2025T2', '2025T1')) AS enrollment_term_count,
        COUNT(*) AS enrollment_count,
        COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM joined_course s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
-- Subtotals with CUBE
SELECT
    enrollment_term_count,
    enrollment_count_2,
    GROUPING(enrollment_term_count) AS is_total_enrollment_term_count,
    ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list,
    ROUND(100 * COUNT(CASE WHEN LENGTH(TRIM(course_code)) >= 4 THEN 1 END) / COUNT(*), 1) AS pct_length_trim_course_code_4,
    COUNT(DISTINCT student_id) AS distinct_student_ids
FROM profiled
GROUP BY CUBE(enrollment_term_count, enrollment_count_2)
