WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score_filled,
    COALESCE(score, 0.0) AS score_filled_2,
    GREATEST(score, score_filled) AS greatest_score_score_filled
FROM cleaned
