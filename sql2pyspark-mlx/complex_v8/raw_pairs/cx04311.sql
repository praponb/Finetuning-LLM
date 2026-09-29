WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE (grade_status <> 'FAIL' OR TO_DATE(enrolled_at) < DATE '2025-03-14')
),
term_stats AS (
    -- Avg score per term
    SELECT term, AVG(score) AS avg_score
    FROM base
    GROUP BY term
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM base s
    JOIN term_stats g ON g.term = s.term
    WHERE s.score > g.avg_score
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    avg_score,
    score_minus_avg
FROM above_average
