WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE grade_status <> 'PASS'
),
course_id_stats AS (
    -- Avg score per course_id
    SELECT course_id, AVG(score) AS avg_score
    FROM base_enrollments
    GROUP BY course_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM base_enrollments s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score > g.avg_score
)
-- Subtotals with GROUPING SETS
SELECT
    course_id,
    COALESCE(term, 'ALL') AS term,
    student_id,
    GROUPING(course_id) AS is_total_course_id,
    COUNT(DISTINCT term) AS distinct_terms,
    ROUND(SUM(score) / NULLIF(SUM(avg_score), 0), 3) AS score_to_avg_score_ratio,
    ROUND(SUM(avg_score), 2) AS total_avg_score
FROM outliers
GROUP BY GROUPING SETS ((course_id, term, student_id), (course_id), (term))
