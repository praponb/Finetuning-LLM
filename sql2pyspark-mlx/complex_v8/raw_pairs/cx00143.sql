WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CASE WHEN term IN ('2025T2', '2024T1') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group,
        FLOOR(score / 25) * 25 AS score_bucket,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE score <= 90.9
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM filtered_enrollments
    WHERE term_group IS NOT NULL
)
-- Subtotals with ROLLUP
SELECT
    term_or_unknown,
    course_id,
    grade_status,
    GROUPING(term_or_unknown) AS is_total_term_or_unknown,
    SUM(score_bucket) AS total_score_bucket,
    COUNT(*) FILTER (WHERE term = '2025T2') AS n_term_2025t2
FROM tagged
GROUP BY ROLLUP(term_or_unknown, course_id, grade_status)
ORDER BY term_or_unknown NULLS LAST, course_id NULLS LAST, grade_status NULLS LAST
