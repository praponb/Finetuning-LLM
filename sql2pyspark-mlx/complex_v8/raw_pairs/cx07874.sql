WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2024T1', '2025T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group,
        CASE WHEN score >= 78.8 THEN 'HIGH' WHEN score >= 34.2 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND enrolled_at < TIMESTAMP '2025-02-24 13:00:00'
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.term_group,
        s.score_tier,
        s.enrolled_at_day,
        c.credits AS course_credits
    FROM filtered s
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Subtotals with GROUPING SETS
SELECT
    term_group,
    score_tier,
    course_credits,
    COUNT(DISTINCT term) AS distinct_terms,
    COUNT(*) FILTER (WHERE term IN ('2025T2', '2024T2')) AS n_term_in_2025t2_2024t2,
    MAX(enrolled_at) AS last_enrolled_at
FROM with_details_course
GROUP BY GROUPING SETS ((term_group, score_tier, course_credits), (term_group), (score_tier), ())
ORDER BY term_group NULLS LAST, score_tier NULLS LAST, course_credits NULLS LAST
