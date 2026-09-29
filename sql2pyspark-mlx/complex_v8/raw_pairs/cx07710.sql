WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        ROUND(score * 1.07, 2) AS score_adj,
        ABS(score - 59.8) AS score_distance
    FROM enrollments
    WHERE score >= 38.3
),
deduped AS (
    -- Keep the latest row per grade_status
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        score_adj,
        score_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY score_distance DESC, id DESC) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        score_adj,
        COUNT(*) OVER (PARTITION BY course_id, student_id) AS rows_in_course_id_student_id,
        LEAD(score_adj) OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC, id) AS next_score_adj
    FROM deduped
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        rows_in_course_id_student_id,
        next_score_adj,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref,
        SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix,
        ROUND(score / NULLIF(score_filled, 0), 3) AS score_per_score_filled
    FROM ranked
),
latest AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        rows_in_course_id_student_id,
        next_score_adj,
        grade_status_ref,
        grade_status_prefix,
        score_per_score_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY next_score_adj DESC, id DESC) AS recency_rank
        FROM enriched
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, score_filled, next_score_adj
    FROM latest
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (score_filled, next_score_adj)
) unp
