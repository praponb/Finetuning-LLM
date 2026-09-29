WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE score < 83.1
      AND course_id IS NOT NULL
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared_enrollments
    WHERE score > (SELECT AVG(score) FROM prepared_enrollments)
),
derived AS (
    -- Derive helper columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(term_slug), ' ', '_')) AS term_slug_slug
    FROM with_activity
),
term_stats AS (
    -- Avg score per term
    SELECT term, AVG(score) AS avg_score
    FROM derived
    GROUP BY term
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM derived s
    JOIN term_stats g ON g.term = s.term
    WHERE s.score > g.avg_score
)
SELECT
    term,
    course_id,
    GROUPING(term) AS is_total_term,
    ROUND(SUM(score) / NULLIF(SUM(score_minus_avg), 0), 3) AS score_to_score_minus_avg_ratio,
    COUNT(*) AS row_count
FROM vs_group
GROUP BY GROUPING SETS ((term, course_id), (term), ())
ORDER BY term NULLS LAST, course_id NULLS LAST
