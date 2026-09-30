WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('-', grade_status, term) AS grade_status_term_label,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND score <= 71.0
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned
    WHERE score > (SELECT AVG(score) FROM cleaned)
)
SELECT *
FROM (
    SELECT student_id, enrolled_at_date, grade_status, score
    FROM screened
) src
PIVOT (
    SUM(score)
    FOR grade_status IN ('FAIL', 'INCOMPLETE')
)
ORDER BY student_id, enrolled_at_date
