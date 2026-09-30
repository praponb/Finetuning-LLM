WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE term IN ('2025T1', '2024T2')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at_quarter,
        st.is_international AS student_is_international,
        c.course_code,
        c.level AS course_level
    FROM recent s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
joined_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at_quarter,
        s.student_is_international,
        s.course_code,
        c.course_code AS course_course_code,
        c.dept AS course_dept,
        c.level AS course_level_2,
        st.enrolled_on AS student_enrolled_on
    FROM with_parent s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        score,
        enrolled_at_quarter,
        student_is_international,
        course_code,
        course_course_code,
        course_dept,
        course_level_2,
        student_enrolled_on,
        MONTH(student_enrolled_on) AS student_enrolled_on_month
    FROM joined_course
),
enriched AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at_quarter,
        s.student_is_international,
        s.course_dept,
        s.course_level_2,
        s.student_enrolled_on,
        st.enrolled_on AS student_enrolled_on_2,
        st.student_name,
        st.cohort AS student_cohort
    FROM scored s
    JOIN students st ON st.id = s.student_id
    WHERE LOWER(st.student_name) LIKE '%ha%'
)
-- Final projection
SELECT
    id,
    student_id,
    enrolled_at_quarter,
    course_dept,
    student_enrolled_on,
    student_enrolled_on_2,
    student_name,
    student_cohort,
    CASE WHEN enrolled_at_quarter >= 4 THEN 'HIGH' WHEN enrolled_at_quarter >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS enrolled_at_quarter_tier,
    DATE_TRUNC('WEEK', student_enrolled_on_2) AS student_enrolled_on_2_week
FROM enriched
ORDER BY course_dept DESC
