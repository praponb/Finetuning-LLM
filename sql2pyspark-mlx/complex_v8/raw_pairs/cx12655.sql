WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label
    FROM enrollments
    WHERE score >= 26.2
),
latest_per_key AS (
    -- Keep the latest row per student_id, term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, term ORDER BY score DESC, id DESC) AS recency_rank
        FROM filtered_enrollments
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(term, 'ALL') AS term,
    course_id,
    ROUND(AVG(score), 2) AS avg_score,
    SUM(CASE WHEN term IS NOT NULL THEN score ELSE 0 END) AS score_term_is_not_null,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list
FROM latest_per_key
GROUP BY GROUPING SETS ((term, course_id), (term))
ORDER BY term NULLS LAST, course_id NULLS LAST
