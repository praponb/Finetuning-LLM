WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score <= 57.6
),
annotated AS (
    -- Label and score rows
    SELECT *, FLOOR(score / 25) * 25 AS score_bucket
    FROM prepared
    WHERE score >= 10.1
)
-- Subtotals with GROUPING SETS
SELECT
    course_id,
    student_id,
    ROUND(SUM(score) / NULLIF(SUM(score_bucket), 0), 3) AS score_to_score_bucket_ratio,
    COUNT(DISTINCT term) AS distinct_terms
FROM annotated
GROUP BY GROUPING SETS ((course_id, student_id), (course_id))
ORDER BY course_id NULLS LAST, student_id NULLS LAST
