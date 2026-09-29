WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE student_id >= 18
      AND course_id >= 4
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM valid_enrollments
),
course_id_stats AS (
    -- Avg score per course_id
    SELECT course_id, AVG(score) AS avg_score
    FROM scored
    GROUP BY course_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM scored s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score > g.avg_score
),
enriched AS (
    -- Label and score rows
    SELECT *, LOWER(TRIM(term_or_unknown)) AS term_or_unknown_clean
    FROM above_average
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    term_or_unknown,
    avg_score,
    score_minus_avg,
    term_or_unknown_clean
FROM enriched
