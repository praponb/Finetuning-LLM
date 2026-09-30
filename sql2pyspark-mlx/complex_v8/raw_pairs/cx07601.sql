WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 59.8 THEN 'A' WHEN score >= 32.7 THEN 'B' ELSE 'C' END AS score_tier,
        ROUND(score * 1.07, 2) AS score_adj
    FROM enrollments
    WHERE course_id <= 9
),
course_id_stats AS (
    -- Max score per course_id
    SELECT course_id, MAX(score) AS max_score
    FROM recent_enrollments
    GROUP BY course_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_score, ROUND(s.score - g.max_score, 2) AS score_minus_max
    FROM recent_enrollments s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score = g.max_score
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_at, score_tier, score_minus_max
    FROM outliers
) src
PIVOT (
    AVG(score_minus_max)
    FOR score_tier IN ('C', 'B', 'A')
)
ORDER BY enrolled_at
