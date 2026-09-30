WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref,
        CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        MONTH(enrolled_at) AS enrolled_at_month
    FROM enrollments
    WHERE score < 62.5
      AND grade_status = 'INCOMPLETE'
),
joined_student AS (
    -- Join students
    SELECT
        s.*,
        st.gpa AS student_gpa,
        st.student_name,
        st.cohort AS student_cohort
    FROM valid_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(enrolled_at_month) IGNORE NULLS OVER (PARTITION BY enrolled_at_month, grade_status ORDER BY enrolled_at_month ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at_month,
        SUM(student_gpa) OVER (PARTITION BY grade_status_group) AS grade_status_group_sum_student_gpa,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY enrolled_at_month, grade_status ORDER BY enrolled_at_month ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        SUM(enrolled_at_month) OVER w_grade_status_group AS grade_status_group_sum_enrolled_at_month,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY enrolled_at_month, grade_status ORDER BY enrolled_at_month ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score
    FROM joined_student
    WINDOW w_grade_status_group AS (PARTITION BY grade_status_group)
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term = '2024T2'
        )
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    score,
    grade_status,
    enrolled_at,
    term_ref,
    grade_status_group,
    enrolled_at_month,
    student_gpa,
    student_name,
    last_known_enrolled_at_month,
    grade_status_group_sum_student_gpa,
    first_known_enrolled_at,
    grade_status_group_sum_enrolled_at_month,
    last_known_score,
    SUBSTRING(TRIM(term), 1, 4) AS term_prefix
FROM screened
WHERE MONTH(first_known_enrolled_at) IN (1, 3)
ORDER BY term, id
