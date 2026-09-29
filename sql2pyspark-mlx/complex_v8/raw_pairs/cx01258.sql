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
        YEAR(enrolled_at) AS enrolled_at_year,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE score > 11.3
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM recent
    WHERE score > (SELECT MIN(score) FROM recent)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, days_since_enrolled_at, enrolled_at_year
    FROM screened
) src
UNPIVOT (
    reading FOR measure IN (days_since_enrolled_at, enrolled_at_year)
) unp
