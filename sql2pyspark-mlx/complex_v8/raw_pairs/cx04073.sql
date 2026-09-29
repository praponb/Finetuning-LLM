WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE score <= 59.7
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international,
        c.course_code,
        c.level AS course_level
    FROM base_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Subtotals with GROUPING SETS
SELECT
    student_is_international,
    COALESCE(term, 'ALL') AS term,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS n_rows,
    MIN(enrolled_at) AS first_enrolled_at,
    MIN(score) AS min_score
FROM enriched
GROUP BY GROUPING SETS ((student_is_international, term), (student_is_international))
ORDER BY student_is_international NULLS LAST, term NULLS LAST
