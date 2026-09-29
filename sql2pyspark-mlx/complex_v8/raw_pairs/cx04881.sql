WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT *
    FROM enrollments
    WHERE (grade_status IS NOT NULL OR course_id >= 5)
),
with_parent AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        st.major AS student_major,
        c.level AS course_level,
        c.credits AS course_credits
    FROM valid_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        YEAR(enrolled_at) AS enrolled_at_year,
        CONCAT_WS('-', grade_status, student_major) AS grade_status_student_major_label,
        COALESCE(course_credits, 0) AS course_credits_filled
    FROM with_parent
    WHERE student_id >= 18
),
enriched_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.student_major,
        s.course_level,
        s.course_credits,
        s.enrolled_at_year,
        s.grade_status_student_major_label,
        s.course_credits_filled,
        st.enrolled_on AS student_enrolled_on
    FROM scored s
    LEFT JOIN students st ON st.id = s.student_id
),
deduped AS (
    -- Keep the latest row per student_major
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        student_major,
        course_level,
        course_credits,
        enrolled_at_year,
        grade_status_student_major_label,
        course_credits_filled,
        student_enrolled_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_major ORDER BY student_enrolled_on DESC, id DESC) AS row_num
        FROM enriched_student
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.student_major,
        s.course_level,
        s.course_credits,
        s.enrolled_at_year,
        s.grade_status_student_major_label,
        s.course_credits_filled,
        s.student_enrolled_on,
        st.student_name,
        st.is_international AS student_is_international,
        c.level AS course_level_2
    FROM deduped s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
    WHERE NOT st.is_international
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(course_level_2), 1, 2) AS course_level_2_prefix,
        enrolled_at_year - course_credits AS enrolled_at_year_minus_course_credits
    FROM enriched
)
-- Final projection
SELECT
    id,
    course_id,
    enrolled_at,
    student_major,
    course_level,
    course_credits,
    course_credits_filled,
    student_name,
    course_level_2,
    course_level_2_prefix,
    enrolled_at_year_minus_course_credits
FROM derived
ORDER BY course_level_2 DESC
