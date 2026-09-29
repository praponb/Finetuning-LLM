WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        YEAR(enrolled_at) AS enrolled_at_year,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE term <> '2025T2'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN term IN ('2024T1', '2024T2') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM filtered
)
SELECT
    grade_status,
    student_id,
    GROUPING_ID() AS gid,
    SUM(enrolled_at_year) FILTER (WHERE term <> '2024T1') AS enrolled_at_year_term_2024t1,
    COUNT(*) AS cnt
FROM labeled
GROUP BY ROLLUP(grade_status, student_id)
ORDER BY grade_status NULLS LAST, student_id NULLS LAST
