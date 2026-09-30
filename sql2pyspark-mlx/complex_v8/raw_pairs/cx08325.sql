WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE term IN ('2025T2', '2025T1', '2024T1')
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(score / 25) * 25 AS score_bucket,
        COALESCE(score, 0.0) AS score_filled,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM cleaned
)
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    score_bucket,
    score_filled,
    grade_status_ref,
    score_filled + score AS score_filled_plus_score,
    ROUND(score / NULLIF(score_bucket, 0), 3) AS score_per_score_bucket
FROM derived
ORDER BY score_filled DESC NULLS FIRST, id
LIMIT 20
