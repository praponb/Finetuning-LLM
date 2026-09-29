WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE term IS NOT NULL
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN score >= 58.8 THEN 'A' WHEN score >= 35.8 THEN 'B' ELSE 'C' END AS score_tier,
        SUBSTRING(TRIM(grade_status_term_label), 1, 4) AS grade_status_term_label_prefix,
        ROUND((UNIX_TIMESTAMP(enrolled_at_plus_30m) - UNIX_TIMESTAMP(enrolled_at)) / 3600, 1) AS hours_enrolled_at_to_enrolled_at_plus_30m
    FROM valid
    WHERE term NOT IN ('2025T1', '2024T2')
)
SELECT id, field, metric_value
FROM (
    SELECT id, hours_enrolled_at_to_enrolled_at_plus_30m, score
    FROM labeled
) src
UNPIVOT (
    metric_value FOR field IN (hours_enrolled_at_to_enrolled_at_plus_30m, score)
) unp
ORDER BY id, field
