WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
),
with_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.term_slug,
        st.gpa AS student_gpa,
        st.major AS student_major
    FROM scoped_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
with_parent AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.term_slug,
        s.student_gpa,
        s.student_major,
        st.gpa AS student_gpa_2,
        c.credits AS course_credits,
        c.level AS course_level
    FROM with_details s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
    WHERE st.gpa >= 1.7
),
latest_per_key AS (
    -- Keep the latest row per grade_status
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        term_slug,
        student_gpa,
        student_major,
        student_gpa_2,
        course_credits,
        course_level
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY student_gpa_2 DESC, id DESC) AS recency_rank
        FROM with_parent
    ) ranked
    WHERE recency_rank = 1
),
with_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term_slug,
        s.student_gpa,
        s.student_major,
        s.student_gpa_2,
        s.course_credits,
        s.course_level,
        st.gpa AS student_gpa_3,
        st.major AS student_major_2
    FROM latest_per_key s
    JOIN students st ON st.id = s.student_id
)
SELECT id, metric, reading
FROM (
    SELECT id, student_gpa_3, student_gpa, student_gpa_2
    FROM with_details_student
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (student_gpa_3, student_gpa, student_gpa_2)
) unp
ORDER BY id, metric
