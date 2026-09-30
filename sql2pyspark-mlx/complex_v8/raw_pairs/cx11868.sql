WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        CONCAT_WS(': ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE grade_status IN ('INCOMPLETE', 'WITHDRAWN', 'PASS')
),
with_details_student AS (
    -- Attach student attributes
    SELECT s.*, st.cohort AS student_cohort, c.dept AS course_dept
    FROM recent_enrollments s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
    WHERE st.cohort = 'C2023'
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.enrolled_at_date,
        s.grade_status_term_label,
        s.student_cohort,
        s.course_dept,
        c.dept AS course_dept_2,
        c.credits AS course_credits,
        c.level AS course_level
    FROM with_details_student s
    JOIN courses c ON c.id = s.course_id AND c.credits < 4
    WHERE c.dept <> 'Math'
),
with_details_student_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.enrolled_at_date,
        s.grade_status_term_label,
        s.student_cohort,
        s.course_dept_2,
        s.course_credits,
        s.course_level,
        st.student_name,
        st.gpa AS student_gpa,
        c.credits AS course_credits_2
    FROM with_parent_course s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
)
SELECT
    id,
    enrolled_at,
    enrolled_at_date,
    course_credits,
    course_level,
    student_gpa,
    course_credits_2,
    CASE WHEN course_dept_2 IN ('CS', 'Biz') THEN 'group_a' WHEN course_dept_2 = 'Art' THEN 'group_b' ELSE 'other' END AS course_dept_2_group
FROM with_details_student_2
WHERE enrolled_at < TIMESTAMP '2025-02-18 08:00:00'
