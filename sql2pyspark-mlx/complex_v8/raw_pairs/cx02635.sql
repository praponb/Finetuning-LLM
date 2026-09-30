WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE student_id >= 17
      AND HOUR(enrolled_at) BETWEEN 6 AND 8
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM recent_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score > 11.1
        )
      AND s.score <= 71.0
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN grade_status_or_unknown IN ('UNKNOWN', 'PASS') THEN 'group_a' WHEN grade_status_or_unknown = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_or_unknown_group,
        COALESCE(score, 0.0) AS score_filled
    FROM screened
)
SELECT id, metric_name, value
FROM (
    SELECT id, score_filled, score
    FROM derived
) src
UNPIVOT (
    value FOR metric_name IN (score_filled, score)
) unp
