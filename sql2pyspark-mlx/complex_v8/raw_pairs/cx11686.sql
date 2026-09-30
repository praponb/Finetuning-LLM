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
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        st.enrolled_on AS student_enrolled_on,
        st.student_name
    FROM base s
    LEFT JOIN students st ON st.id = s.student_id
),
with_parent_student_2 AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.student_enrolled_on,
        s.student_name,
        st.gpa AS student_gpa,
        st.student_name AS student_student_name,
        st.cohort AS student_cohort,
        c.dept AS course_dept
    FROM with_parent_student s
    LEFT JOIN students st ON st.id = s.student_id AND st.cohort = 'C2023'
    JOIN courses c ON c.id = s.course_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN student_gpa >= 3.2 THEN 'A' WHEN student_gpa >= 2.5 THEN 'B' ELSE 'C' END AS student_gpa_tier,
        ROUND(score / NULLIF(student_gpa, 0), 3) AS score_per_student_gpa
    FROM with_parent_student_2
    WHERE LOWER(student_name) LIKE '%n %'
)
SELECT
    id,
    score,
    student_enrolled_on,
    student_name,
    student_gpa,
    student_student_name,
    student_cohort,
    student_gpa_tier,
    score_per_student_gpa,
    LOWER(TRIM(student_student_name)) AS student_student_name_clean,
    LEAST(score, student_gpa) AS least_score_student_gpa
FROM tagged
ORDER BY student_enrolled_on NULLS LAST, student_name DESC
