WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 6 AND 10
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(score / NULLIF(grade_status_len, 0), 3) AS score_per_grade_status_len,
        COALESCE(grade_status_len, 0) AS grade_status_len_filled
    FROM base
    WHERE course_id IS NOT NULL
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM labeled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score > 38.5
        )
),
student_id_stats AS (
    -- Avg score_per_grade_status_len per student_id
    SELECT student_id, AVG(score_per_grade_status_len) AS avg_score_per_grade_status_len
    FROM screened
    GROUP BY student_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_score_per_grade_status_len,
        ROUND(s.score_per_grade_status_len - g.avg_score_per_grade_status_len, 2) AS score_per_grade_status_len_minus_avg
    FROM screened s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score_per_grade_status_len > g.avg_score_per_grade_status_len
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY grade_status_or_unknown) AS rows_in_grade_status_or_unknown,
        SUM(grade_status_len) OVER (PARTITION BY student_id, grade_status_or_unknown ORDER BY score_per_grade_status_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_grade_status_len
    FROM outliers
),
screened_2 AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_windows s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.score >= 17.4
              AND e.term <> '2024T2'
        )
)
-- Final projection
SELECT
    id,
    score,
    enrolled_at,
    grade_status_or_unknown,
    grade_status_len,
    score_per_grade_status_len,
    avg_score_per_grade_status_len,
    rows_in_grade_status_or_unknown,
    running_grade_status_len
FROM screened_2
ORDER BY score NULLS LAST, running_grade_status_len
