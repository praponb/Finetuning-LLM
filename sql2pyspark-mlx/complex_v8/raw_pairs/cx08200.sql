WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        CASE WHEN term IN ('2024T1', '2024T2') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE grade_status IN ('FAIL', 'PASS')
      AND term = '2024T2'
),
joined_student AS (
    -- Attach student attributes
    SELECT
        s.*,
        st.student_name,
        c.credits AS course_credits,
        c.level AS course_level
    FROM filtered_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        enrolled_at + INTERVAL 1 DAYS AS enrolled_at_plus_1d,
        score - course_credits AS score_minus_course_credits
    FROM joined_student
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM derived s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.grade_status <> 'WITHDRAWN'
        )
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(term_group, 'ALL') AS term_group,
    COALESCE(grade_status_or_unknown, 'ALL') AS grade_status_or_unknown,
    COALESCE(grade_status, 'ALL') AS grade_status,
    GROUPING_ID() AS grouping_level,
    SUM(CASE WHEN student_name IS NOT NULL THEN score_minus_course_credits ELSE 0 END) AS score_minus_course_credits_student_name_is_not_n,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list
FROM screened
GROUP BY GROUPING SETS ((term_group, grade_status_or_unknown, grade_status), (term_group), (grade_status_or_unknown), ())
