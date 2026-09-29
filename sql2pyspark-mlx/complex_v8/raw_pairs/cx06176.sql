WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE grade_status = 'FAIL'
),
enriched_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.grade_status_term_label,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international
    FROM scoped s
    JOIN students st ON st.id = s.student_id AND st.cohort NOT IN ('C2022', 'C2024')
    WHERE NOT st.is_international
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN term IN ('2025T2', '2024T2') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group,
        CONCAT(grade_status_term_label, '#', CAST(id AS STRING)) AS grade_status_term_label_ref,
        student_gpa - score AS student_gpa_minus_score
    FROM enriched_student
    WHERE grade_status_term_label IS NOT NULL
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND MONTH(e.enrolled_at) IN (1, 2)
              AND e.term IS NOT NULL
        )
)
-- Subtotals with GROUPING SETS
SELECT
    course_id,
    term_group,
    student_is_international,
    GROUPING(course_id) AS is_total_course_id,
    COUNT(DISTINCT student_id) AS distinct_student_ids,
    COUNT(*) AS row_count
FROM without_matches
GROUP BY GROUPING SETS ((course_id, term_group, student_is_international), (course_id), (term_group))
ORDER BY course_id NULLS LAST, term_group NULLS LAST, student_is_international NULLS LAST
