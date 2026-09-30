WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        ROUND(score * 0.9, 2) AS score_adj
    FROM enrollments
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        score,
        grade_status,
        score_adj,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        score - score_adj AS score_minus_score_adj,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref
    FROM recent_enrollments
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM tagged
    WHERE score_adj > (SELECT MIN(score_adj) FROM tagged)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        score,
        grade_status,
        score_adj,
        grade_status_or_unknown,
        score_minus_score_adj,
        term_ref,
        ROUND(score / NULLIF(score_minus_score_adj, 0), 3) AS score_per_score_minus_score_adj,
        CASE WHEN score >= 74.9 THEN 'A' WHEN score >= 35.8 THEN 'B' ELSE 'C' END AS score_tier,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown_2
    FROM screened
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT grade_status, score_tier, score
    FROM annotated
) src
PIVOT (
    MAX(score)
    FOR score_tier IN ('B', 'C', 'A')
)
ORDER BY grade_status
