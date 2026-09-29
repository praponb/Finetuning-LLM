WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE course_id >= 2
),
unique_rows AS (
    -- Keep the latest row per term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY score DESC, id DESC) AS recency_rank
        FROM recent
    ) ranked
    WHERE recency_rank = 1
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref,
        enrolled_at + INTERVAL 7 DAYS AS enrolled_at_plus_7d
    FROM unique_rows
)
SELECT
    grade_status,
    course_id,
    COUNT(DISTINCT id) AS distinct_ids
FROM derived
GROUP BY GROUPING SETS ((grade_status, course_id), (grade_status), (course_id), ())
ORDER BY grade_status NULLS LAST, course_id NULLS LAST
