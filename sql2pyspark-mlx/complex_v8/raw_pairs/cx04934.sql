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
        ABS(score - 57.6) AS score_distance
    FROM enrollments
    WHERE course_id >= 4
      AND term NOT IN ('2025T2', '2024T1')
),
deduped AS (
    -- Keep the latest row per term, grade_status
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, grade_status ORDER BY enrolled_at DESC, id DESC) AS row_num
        FROM recent_enrollments
    ) ranked
    WHERE row_num = 1
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score <= 57.0
        )
),
qualified_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE score > (SELECT AVG(score) FROM qualified)
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    course_id,
    student_id,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(score), 2) AS total_score,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
FROM qualified_2
GROUP BY ROLLUP(grade_status, course_id, student_id)
ORDER BY grade_status NULLS LAST, course_id NULLS LAST, student_id NULLS LAST
