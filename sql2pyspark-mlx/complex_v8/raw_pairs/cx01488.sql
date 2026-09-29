WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('PASS', 'FAIL') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND term <> '2025T1'
),
with_activity AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.dept = 'Bio'
        )
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m,
        ABS(score - 69.6) AS score_distance
    FROM with_activity
    WHERE student_id IS NOT NULL
),
combined_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status_group,
        s.enrolled_at_date,
        s.grade_status_slug,
        s.enrolled_at_plus_30m,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort
    FROM labeled s
    JOIN students st ON st.id = s.student_id
)
-- Subtotals with GROUPING SETS
SELECT
    term,
    student_id,
    student_cohort,
    GROUPING(term) AS is_total_term,
    SUM(student_gpa) AS total_student_gpa,
    MIN(enrolled_at_plus_30m) AS first_enrolled_at_plus_30m
FROM combined_details_student
GROUP BY GROUPING SETS ((term, student_id, student_cohort), (term), (student_id))
