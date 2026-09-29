WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        COALESCE(score, 0.0) AS score_filled,
        ROUND(score * 1.1, 2) AS score_adj,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE student_id >= 4
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM prepared_enrollments s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE term <> '2025T1') AS enrollment_term_count,
        COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM summarized_rows s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.course_id
),
with_details AS (
    -- Join students
    SELECT
        s.*,
        st.gpa AS student_gpa,
        st.student_name,
        st.major AS student_major
    FROM with_stats s
    JOIN students st ON st.id = s.student_id
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.score_adj,
        s.grade_status_term_label,
        s.enrollment_term_count,
        s.enrollment_count_2,
        s.student_gpa,
        s.student_name,
        s.student_major,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.term AS enrollment_term
    FROM with_details s
    JOIN enrollments e ON e.course_id = s.course_id
),
combined_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.score_adj,
        s.grade_status_term_label,
        s.enrollment_term_count,
        s.enrollment_count_2,
        s.student_gpa,
        s.student_name,
        s.student_major,
        s.enrollment_id,
        s.enrollment_score,
        st.student_name AS student_student_name,
        st.major AS student_major_2,
        e.score AS enrollment_score_2,
        e.term AS enrollment_term_2
    FROM with_children s
    JOIN students st ON st.id = s.student_id
    JOIN enrollments e ON e.id = s.enrollment_id
),
with_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.grade_status_term_label,
        s.enrollment_term_count,
        s.enrollment_count_2,
        s.student_gpa,
        s.student_name,
        s.student_major,
        s.enrollment_id,
        s.student_student_name,
        s.student_major_2,
        s.enrollment_score_2,
        st.major AS student_major_3,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM combined_details_student s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
    WHERE st.major = 'Math'
)
-- Final projection
SELECT
    id,
    student_id,
    grade_status,
    grade_status_term_label,
    enrollment_term_count,
    enrollment_count_2,
    student_gpa,
    student_name,
    enrollment_id,
    student_student_name,
    student_major_2,
    enrollment_score_2,
    student_major_3,
    ROUND(enrollment_score_2 / NULLIF(enrollment_term_count, 0), 3) AS enrollment_score_2_per_enrollment_term_count,
    DATE_FORMAT(enrollment_enrolled_at, 'yyyy-MM-dd HH:00') AS enrollment_enrolled_at_label
FROM with_details_student
ORDER BY enrollment_score_2_per_enrollment_term_count, enrollment_enrolled_at_label
