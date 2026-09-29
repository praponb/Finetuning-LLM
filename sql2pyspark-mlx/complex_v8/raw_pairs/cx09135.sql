WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score < 59.7
),
joined AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        c.course_code,
        c.credits AS course_credits,
        st.cohort AS student_cohort,
        st.gpa AS student_gpa
    FROM scoped s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.credits < 4
),
unique_rows AS (
    -- Keep the latest row per student_cohort
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        course_code,
        course_credits,
        student_cohort,
        student_gpa
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_cohort ORDER BY course_credits DESC, id DESC) AS row_num
        FROM joined
    ) ranked
    WHERE row_num = 1
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.course_code,
        s.course_credits,
        s.student_cohort,
        s.student_gpa,
        c.level AS course_level,
        c.dept AS course_dept,
        st.gpa AS student_gpa_2
    FROM unique_rows s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    student_id,
    course_id,
    score,
    course_code,
    student_cohort,
    student_gpa,
    course_dept,
    student_gpa_2
FROM with_parent
WHERE course_dept IS NOT NULL
