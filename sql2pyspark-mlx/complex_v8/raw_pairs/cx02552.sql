WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 9 AND 15
),
screened AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM prepared_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.cohort IS NOT NULL
              AND st.gpa > 2.0
        )
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('|', grade_status, grade_status_term_label) AS grade_status_grade_status_term_label_label,
        DATE_TRUNC('HOUR', enrolled_at) AS enrolled_at_hour
    FROM screened
    WHERE grade_status_term_label IS NOT NULL
),
joined AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.grade_status_term_label,
        s.grade_status_grade_status_term_label_label,
        s.enrolled_at_hour,
        st.cohort AS student_cohort,
        st.major AS student_major,
        st.student_name,
        c.credits AS course_credits
    FROM labeled s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
),
labeled_2 AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN course_credits >= 4 THEN 'A' WHEN course_credits >= 2 THEN 'B' ELSE 'C' END AS course_credits_tier,
        CASE WHEN course_credits >= 4 THEN 'A' WHEN course_credits >= 2 THEN 'B' ELSE 'C' END AS course_credits_tier_2
    FROM joined
    WHERE grade_status_grade_status_term_label_label IS NOT NULL
),
with_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status_term_label,
        s.grade_status_grade_status_term_label_label,
        s.enrolled_at_hour,
        s.student_cohort,
        s.student_major,
        s.student_name,
        s.course_credits_tier_2,
        st.major AS student_major_2,
        st.student_name AS student_student_name,
        st.gpa AS student_gpa,
        c.course_code,
        c.dept AS course_dept
    FROM labeled_2 s
    JOIN students st ON st.id = s.student_id AND st.cohort <> 'C2021'
    LEFT JOIN courses c ON c.id = s.course_id
)
SELECT
    id,
    student_id,
    course_id,
    grade_status_term_label,
    enrolled_at_hour,
    student_name,
    course_credits_tier_2,
    student_student_name,
    student_gpa,
    course_dept,
    COALESCE(student_major_2, 'UNKNOWN') AS student_major_2_or_unknown,
    ROUND(student_gpa * 1.07, 2) AS student_gpa_adj
FROM with_details_student
ORDER BY student_gpa DESC NULLS FIRST, course_dept
