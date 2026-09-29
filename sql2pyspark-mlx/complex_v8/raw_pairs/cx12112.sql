WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score < 81.4
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        grade_status,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM prepared
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE score_bucket > (SELECT AVG(score_bucket) FROM labeled)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT student_id, grade_status, score_bucket
    FROM screened
) src
PIVOT (
    SUM(score_bucket)
    FOR grade_status IN ('PASS', 'WITHDRAWN')
)
ORDER BY student_id
