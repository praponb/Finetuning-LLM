WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM enrollments
    WHERE term = '2025T2'
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM recent
    WHERE score < (SELECT MAX(score) FROM recent)
),
enriched AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM without_matches
)
SELECT *
FROM (
    SELECT student_id, grade_status, score
    FROM enriched
) src
PIVOT (
    MAX(score)
    FOR grade_status IN ('WITHDRAWN', 'FAIL', 'INCOMPLETE')
)
ORDER BY student_id
