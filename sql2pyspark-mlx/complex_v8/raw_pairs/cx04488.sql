WITH base_enrollments AS (
    -- Filter enrollments
    SELECT *
    FROM enrollments
    WHERE student_id >= 17
),
deduped AS (
    -- Keep the latest row per term, student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, student_id ORDER BY enrolled_at DESC, id DESC) AS recency_rank
        FROM base_enrollments
    ) ranked
    WHERE recency_rank = 1
),
combined_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        st.major AS student_major,
        c.dept AS course_dept
    FROM deduped s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
),
joined_student AS (
    -- Join students
    SELECT
        s.*,
        st.student_name,
        st.cohort AS student_cohort,
        st.enrolled_on AS student_enrolled_on
    FROM combined_details_student s
    JOIN students st ON st.id = s.student_id
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.student_name,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status
    FROM joined_student s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM line_level
    WHERE enrollment_score > (SELECT AVG(enrollment_score) FROM line_level)
)
-- Final projection
SELECT
    id,
    course_id,
    enrolled_at,
    student_name,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_score,
    enrollment_grade_status,
    SUBSTRING(TRIM(enrollment_grade_status), 1, 4) AS enrollment_grade_status_prefix
FROM filtered
ORDER BY enrollment_id DESC, id
LIMIT 10
