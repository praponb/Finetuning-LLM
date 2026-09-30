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
        ROUND(score * 1.1, 2) AS score_adj
    FROM enrollments
    WHERE student_id IS NOT NULL
),
joined AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_adj,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort,
        st.major AS student_major,
        c.level AS course_level,
        c.dept AS course_dept
    FROM base s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
enriched_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_adj,
        s.student_gpa,
        s.student_cohort,
        s.student_major,
        s.course_dept,
        c.credits AS course_credits,
        c.level AS course_level_2,
        st.cohort AS student_cohort_2,
        st.major AS student_major_2
    FROM joined s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
    WHERE c.level IS NOT NULL
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        score,
        grade_status,
        enrolled_at,
        score_adj,
        student_gpa,
        student_major,
        course_level_2,
        student_major_2,
        CONCAT_WS('-', grade_status, student_cohort_2) AS grade_status_student_cohort_2_label,
        COALESCE(score_adj, 0.0) AS score_adj_filled
    FROM enriched_course
    WHERE student_cohort_2 IN ('C2021', 'C2023')
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) AS enrollment_count,
        MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.first_enrollment_enrolled_at
    FROM annotated s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.score,
        s.enrolled_at,
        s.student_major,
        s.course_level_2,
        s.student_major_2,
        s.grade_status_student_cohort_2_label,
        s.enrollment_count,
        s.first_enrollment_enrolled_at,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status
    FROM with_stats s
    JOIN enrollments e ON e.student_id = s.student_id AND e.course_id >= 10
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        score,
        enrolled_at,
        course_level_2,
        student_major_2,
        grade_status_student_cohort_2_label,
        enrollment_count,
        first_enrollment_enrolled_at,
        enrollment_id,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY DATE_TRUNC('MONTH', first_enrollment_enrolled_at) ORDER BY enrolled_at, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', first_enrollment_enrolled_at) ORDER BY enrolled_at, id, enrollment_id) AS enrolled_at_seq
    FROM exploded_detail
)
SELECT *
FROM (
    SELECT student_major_2, enrollment_id, course_level_2, score
    FROM with_windows
) src
PIVOT (
    SUM(score)
    FOR course_level_2 IN ('core', 'advanced', 'intro')
)
