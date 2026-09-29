WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND grade_status IS NOT NULL
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term
    FROM filtered_enrollments s
    JOIN enrollments e ON e.course_id = s.course_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        UPPER(TRIM(enrollment_term)) AS enrollment_term_clean,
        ROUND(enrollment_score * 0.9, 2) AS enrollment_score_adj
    FROM with_children_enrollments
    WHERE score > 38.3
)
SELECT id, enrollment_id, measure, metric_value
FROM (
    SELECT id, enrollment_id, score, enrollment_score, enrollment_score_adj
    FROM tagged
) src
UNPIVOT (
    metric_value FOR measure IN (score, enrollment_score, enrollment_score_adj)
) unp
ORDER BY id, enrollment_id, measure
