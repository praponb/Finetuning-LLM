WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE score > 13.9
      AND student_id IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(': ', grade_status_clean, term) AS grade_status_clean_term_label
    FROM recent
    WHERE grade_status IS NOT NULL
)
SELECT *
FROM (
    SELECT course_id, grade_status, score
    FROM enriched
) src
PIVOT (
    AVG(score) AS avg_v, COUNT(score) AS n
    FOR grade_status IN ('INCOMPLETE', 'PASS', 'WITHDRAWN')
)
ORDER BY course_id
