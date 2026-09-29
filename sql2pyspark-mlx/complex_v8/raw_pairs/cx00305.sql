WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 49.0) AS score_distance,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix
    FROM enrollments
    WHERE term NOT IN ('2025T1', '2025T2')
      AND grade_status NOT IN ('INCOMPLETE', 'PASS')
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status_ref,
        s.term_prefix,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term
    FROM filtered s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        c.course_code,
        c.dept AS course_dept,
        c.level AS course_level,
        st.enrolled_on AS student_enrolled_on,
        st.is_international AS student_is_international
    FROM with_children_enrollments s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        grade_status_ref,
        enrollment_id,
        enrollment_grade_status,
        course_code,
        course_dept,
        course_level,
        student_is_international,
        LAG(student_enrolled_on, 3) OVER w_student_id_by_student_enroll AS prev3_student_enrolled_on,
        NTH_VALUE(student_enrolled_on, 2) OVER (PARTITION BY student_id ORDER BY student_enrolled_on DESC, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_enrolled_on,
        LAG(student_enrolled_on) OVER (PARTITION BY student_id ORDER BY student_enrolled_on DESC, id, enrollment_id) AS prev_student_enrolled_on
    FROM combined_details
    WINDOW w_student_id_by_student_enroll AS (PARTITION BY student_id ORDER BY student_enrolled_on DESC, id, enrollment_id)
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(course_level, 'ALL') AS course_level,
    COALESCE(enrollment_grade_status, 'ALL') AS enrollment_grade_status,
    course_id,
    MIN(prev_student_enrolled_on) AS first_prev_student_enrolled_on,
    COUNT(*) FILTER (WHERE LENGTH(TRIM(course_code)) >= 7) AS n_length_trim_course_code_7
FROM sequenced
GROUP BY GROUPING SETS ((course_level, enrollment_grade_status, course_id), (course_level), ())
ORDER BY course_level NULLS LAST, enrollment_grade_status NULLS LAST, course_id NULLS LAST
