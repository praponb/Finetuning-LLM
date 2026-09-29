WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CASE WHEN score >= 59.7 THEN 'A' WHEN score >= 20.2 THEN 'B' ELSE 'C' END AS score_tier,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE grade_status = 'PASS'
),
unique_rows AS (
    -- Keep the earliest row per score_tier, course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        score_tier,
        grade_status_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY score_tier, course_id ORDER BY score, id) AS recency_rank
        FROM recent_enrollments
    ) ranked
    WHERE recency_rank = 1
),
unique_rows_2 AS (
    -- Keep the latest row per student_id, score_tier
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        score_tier,
        grade_status_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, score_tier ORDER BY enrolled_at DESC, id DESC) AS row_num
        FROM unique_rows
    ) ranked
    WHERE row_num = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT course_id, score_tier, score
    FROM unique_rows_2
) src
PIVOT (
    MAX(score)
    FOR score_tier IN ('C', 'A', 'B')
)
ORDER BY course_id
