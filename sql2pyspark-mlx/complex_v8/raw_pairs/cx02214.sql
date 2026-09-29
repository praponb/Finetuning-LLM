WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score > 42.4
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref,
        ABS(score - 38.3) AS score_distance,
        ABS(score - 55.3) AS score_distance_2
    FROM recent
),
combined_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.grade_status_ref,
        s.score_distance,
        s.score_distance_2,
        c.credits AS course_credits
    FROM derived s
    JOIN courses c ON c.id = s.course_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, score_distance, score_distance_2
    FROM combined_details_course
) src
UNPIVOT (
    value FOR field IN (score_distance, score_distance_2)
) unp
ORDER BY id, field
