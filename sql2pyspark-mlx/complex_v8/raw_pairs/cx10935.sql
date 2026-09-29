WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label_2,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE (student_id IS NOT NULL OR enrolled_at < TIMESTAMP '2025-02-12 12:00:00')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(enrolled_at) OVER (ORDER BY enrolled_at, id) AS next_enrolled_at,
        NTILE(5) OVER (ORDER BY enrolled_at, id) AS enrolled_at_quintile
    FROM base_enrollments
),
enriched_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.grade_status_term_label,
        s.grade_status_term_label_2,
        s.next_enrolled_at,
        s.enrolled_at_quintile,
        st.is_international AS student_is_international,
        st.student_name
    FROM windowed s
    JOIN students st ON st.id = s.student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM enriched_student s
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.grade_status_term_label,
        s.grade_status_term_label_2,
        s.next_enrolled_at,
        s.student_name,
        s.enrollment_count,
        st.major AS student_major,
        st.enrolled_on AS student_enrolled_on
    FROM with_stats s
    JOIN students st ON st.id = s.student_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        grade_status_term_label,
        grade_status_term_label_2,
        student_name,
        enrollment_count,
        student_major,
        student_enrolled_on,
        DENSE_RANK() OVER (PARTITION BY student_major, term ORDER BY enrollment_count DESC) AS enrollment_count_dense_rank,
        LAG(enrollment_count) OVER w_all_by_student_enrolled_on AS prev_enrollment_count
    FROM joined
    WINDOW w_all_by_student_enrolled_on AS (ORDER BY student_enrolled_on ASC NULLS LAST, id)
)
-- Subtotals with CUBE
SELECT
    student_id,
    course_id,
    MAX(enrollment_count_dense_rank) AS max_enrollment_count_dense_rank,
    SUM(enrollment_count_dense_rank) AS total_enrollment_count_dense_rank
FROM metrics
GROUP BY CUBE(student_id, course_id)
