WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND HOUR(enrolled_at) BETWEEN 7 AND 13
),
course_id_stats AS (
    -- Avg score per course_id
    SELECT course_id, AVG(score) AS avg_score
    FROM recent
    GROUP BY course_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM recent s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score > g.avg_score
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(grade_status_len) OVER (ORDER BY grade_status_len ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS grade_status_len_avg_2,
        RANK() OVER (ORDER BY grade_status_len ASC) AS grade_status_len_rank,
        MIN(avg_score) OVER (PARTITION BY course_id, term) AS course_id_term_min_avg_score
    FROM vs_group
),
with_parent AS (
    -- Attach course attributes
    SELECT
        s.*,
        c.level AS course_level,
        c.credits AS course_credits,
        c.course_code
    FROM ranked s
    JOIN courses c ON c.id = s.course_id
),
qualified AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM with_parent s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.credits BETWEEN 2 AND 4
              AND c.dept IS NOT NULL
        )
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE grade_status_len_avg_2 > (SELECT AVG(grade_status_len_avg_2) FROM qualified)
)
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    term_slug,
    grade_status_len,
    avg_score,
    score_minus_avg,
    grade_status_len_rank,
    course_level,
    course_credits,
    course_code,
    CASE WHEN grade_status_len >= 12 THEN 'A' WHEN grade_status_len >= 6 THEN 'B' ELSE 'C' END AS grade_status_len_tier
FROM with_activity
