WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        CASE WHEN term IN ('2025T2', '2024T1') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE score > 17.4
),
stats AS (
    -- Group-level totals
    SELECT
        term_group,
        grade_status_len,
        student_id,
        COUNT(*) AS row_count,
        COUNT(DISTINCT term_group) AS distinct_term_groups
    FROM base_enrollments
    GROUP BY term_group, grade_status_len, student_id
    HAVING COUNT(*) >= 2
),
with_details_student AS (
    -- Attach student attributes
    SELECT
        s.term_group,
        s.grade_status_len,
        s.student_id,
        s.row_count,
        s.distinct_term_groups,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international
    FROM stats s
    JOIN students st ON st.id = s.student_id
)
SELECT *
FROM (
    SELECT student_id, distinct_term_groups, student_cohort, grade_status_len
    FROM with_details_student
) src
PIVOT (
    SUM(grade_status_len)
    FOR student_cohort IN ('C2023', 'C2024', 'C2021')
)
ORDER BY student_id, distinct_term_groups
