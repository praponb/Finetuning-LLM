WITH valid_enrollments AS (
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
    WHERE (score BETWEEN 20.2 AND 78.8 OR TO_DATE(enrolled_at) < DATE '2025-02-24')
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(score / 25) * 25 AS score_bucket,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM valid_enrollments
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        course_id,
        term,
        enrolled_at,
        term_ref,
        grade_status_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        COALESCE(score_bucket, 0) AS score_bucket_filled,
        ABS(score_bucket - 65) AS score_bucket_distance
    FROM labeled
    WHERE term = '2025T2'
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM tagged s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.score > 17.4
        )
)
SELECT id, field, metric_value
FROM (
    SELECT id, score_bucket_distance, score_bucket_filled
    FROM with_activity
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR field IN (score_bucket_distance AS m_score_bucket_distance, score_bucket_filled AS score_bucket_filled_v)
) unp
