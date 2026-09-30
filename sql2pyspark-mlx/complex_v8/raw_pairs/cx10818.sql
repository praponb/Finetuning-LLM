WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        term,
        score,
        FLOOR(score / 25) * 25 AS score_bucket,
        UPPER(TRIM(grade_status)) AS grade_status_clean,
        ROUND(score * 1.1, 2) AS score_adj
    FROM filtered_enrollments
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM annotated
    WHERE score > (SELECT MIN(score) FROM annotated)
)
SELECT
    id,
    score,
    score_bucket,
    score_adj,
    LENGTH(TRIM(grade_status_clean)) AS grade_status_clean_len
FROM without_matches
ORDER BY score_bucket, id NULLS LAST
LIMIT 10
