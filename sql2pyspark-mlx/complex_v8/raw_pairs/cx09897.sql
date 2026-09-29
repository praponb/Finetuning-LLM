WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
)
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    score + grade_status_len AS score_plus_grade_status_len,
    ROUND(score * 0.25, 2) AS score_adj
FROM prepared
WHERE grade_status_len <= 10
ORDER BY score DESC, student_id
