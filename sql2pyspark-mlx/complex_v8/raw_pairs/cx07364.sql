WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score <= 77.0
      AND enrolled_at < TIMESTAMP '2025-03-08 08:00:00'
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at
    FROM cleaned s
),
combined_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        st.major AS student_major,
        st.is_international AS student_is_international,
        st.student_name
    FROM with_counts s
    JOIN students st ON st.id = s.student_id
    WHERE st.major NOT IN ('Bio', 'Biz')
),
tagged AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(term)) AS term_len
    FROM combined_details_student
    WHERE grade_status NOT IN ('PASS', 'WITHDRAWN')
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    student_major,
    student_is_international,
    student_name,
    term_len,
    ABS(term_len - 8) AS term_len_distance,
    CASE WHEN student_is_international THEN 1 ELSE 0 END AS student_is_international_flag
FROM tagged
ORDER BY grade_status, student_id DESC
