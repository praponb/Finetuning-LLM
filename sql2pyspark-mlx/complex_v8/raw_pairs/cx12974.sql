WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        CASE WHEN score >= 83.1 THEN 'A' WHEN score >= 26.2 THEN 'B' ELSE 'C' END AS score_tier,
        CONCAT_WS('|', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE score < 57.6
),
enriched AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.term_slug,
        s.score_tier,
        s.term_grade_status_label,
        st.gpa AS student_gpa,
        st.student_name,
        c.course_code
    FROM prepared_enrollments s
    JOIN students st ON st.id = s.student_id AND st.is_international = TRUE
    JOIN courses c ON c.id = s.course_id
)
-- Subtotals with ROLLUP
SELECT
    course_id,
    score_tier,
    student_id,
    GROUPING(course_id) AS is_total_course_id,
    MIN(score) AS min_score,
    COUNT(DISTINCT course_id) AS distinct_course_ids
FROM enriched
GROUP BY ROLLUP(course_id, score_tier, student_id)
ORDER BY course_id NULLS LAST, score_tier NULLS LAST, student_id NULLS LAST
