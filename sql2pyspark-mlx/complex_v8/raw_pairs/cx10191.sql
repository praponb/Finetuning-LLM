WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 39.4) AS score_distance,
        CONCAT_WS('|', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND score <= 57.8
),
joined AS (
    -- Join courses
    SELECT
        s.*,
        c.credits AS course_credits,
        c.level AS course_level,
        c.course_code
    FROM cleaned_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
),
joined_student AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.is_international AS student_is_international,
        st.cohort AS student_cohort,
        st.student_name
    FROM joined s
    JOIN students st ON st.id = s.student_id
    WHERE st.cohort = 'C2021'
)
SELECT
    term,
    student_is_international,
    GROUPING_ID() AS grouping_level,
    MAX(enrolled_at) AS last_enrolled_at
FROM joined_student
GROUP BY ROLLUP(term, student_is_international)
ORDER BY term NULLS LAST, student_is_international NULLS LAST
