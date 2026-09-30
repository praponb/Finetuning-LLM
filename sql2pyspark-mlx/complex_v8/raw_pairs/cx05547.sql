WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE grade_status = 'INCOMPLETE'
      AND course_id IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at_label,
        score_filled,
        COALESCE(score_filled, 0.0) AS score_filled_filled,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label_2
    FROM base
    WHERE term NOT IN ('2025T2', '2024T1')
)
SELECT
    course_id,
    grade_status,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(score_filled), 2) AS stddev_score_filled,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
    COUNT(DISTINCT course_id) AS distinct_course_ids
FROM enriched
GROUP BY GROUPING SETS ((course_id, grade_status), (course_id), ())
ORDER BY course_id NULLS LAST, grade_status NULLS LAST
