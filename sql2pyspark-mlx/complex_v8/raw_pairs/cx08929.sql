WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE grade_status IS NOT NULL
      AND score < 78.8
),
combined_details_student AS (
    -- Join students
    SELECT
        s.*,
        st.cohort AS student_cohort,
        st.enrolled_on AS student_enrolled_on,
        st.gpa AS student_gpa
    FROM cleaned s
    LEFT JOIN students st ON st.id = s.student_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        ROUND(SUM(student_gpa) / NULLIF(SUM(score_bucket), 0), 3) AS student_gpa_to_score_bucket_ratio,
        COUNT(*) AS n_rows,
        ROUND(100 * COUNT(CASE WHEN term = '2025T2' THEN 1 END) / COUNT(*), 1) AS pct_term_2025t2,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        MIN(enrolled_at) AS first_enrolled_at
    FROM combined_details_student
    WHERE student_cohort = 'C2021'
    GROUP BY course_id
    HAVING AVG(score_bucket) > 30
),
with_parent AS (
    -- Join courses
    SELECT
        s.course_id,
        s.student_gpa_to_score_bucket_ratio,
        s.n_rows,
        s.pct_term_2025t2,
        s.distinct_student_ids,
        s.first_enrolled_at,
        c.course_code,
        c.level AS course_level,
        c.dept AS course_dept
    FROM per_group s
    JOIN courses c ON c.id = s.course_id
    WHERE c.level IN ('intro', 'core')
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT first_enrolled_at, course_level, student_gpa_to_score_bucket_ratio
    FROM with_parent
) src
PIVOT (
    AVG(student_gpa_to_score_bucket_ratio)
    FOR course_level IN ('core', 'advanced', 'intro')
)
ORDER BY first_enrolled_at
