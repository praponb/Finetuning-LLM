WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE term IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN term IN ('2025T1', '2024T2') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM filtered_enrollments
)
SELECT
    term,
    course_id,
    student_id,
    GROUPING_ID() AS gid,
    MIN(score) AS min_score,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched
GROUP BY GROUPING SETS ((term, course_id, student_id), (term), ())
ORDER BY term NULLS LAST, course_id NULLS LAST, student_id NULLS LAST
