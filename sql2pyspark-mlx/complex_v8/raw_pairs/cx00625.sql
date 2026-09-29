WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term IS NOT NULL
      AND grade_status IN ('WITHDRAWN', 'INCOMPLETE', 'FAIL')
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DENSE_RANK() OVER (PARTITION BY student_id ORDER BY score, id) AS score_dense_rank,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score
    FROM cleaned_enrollments
),
deduped AS (
    -- Keep the latest row per grade_status
    SELECT
        id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_dense_rank,
        first_known_score
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id DESC) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS min_enrollment_score
    FROM deduped s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, score, min_enrollment_score, first_known_score
    FROM with_stats
) src
UNPIVOT (
    metric_value FOR field IN (score AS m_score, min_enrollment_score AS min_enrollment_score_v, first_known_score AS m_first_known_score)
) unp
ORDER BY id, field
