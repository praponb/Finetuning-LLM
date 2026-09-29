WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
),
course_id_stats AS (
    -- Avg score per course_id
    SELECT course_id, AVG(score) AS avg_score
    FROM cleaned
    GROUP BY course_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM cleaned s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score > g.avg_score
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM above_average s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id IS NOT NULL
        )
      AND s.score_minus_avg <= 60.0
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.student_name,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international
    FROM screened s
    JOIN students st ON st.id = s.student_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT course_id, grade_status, score_adj
    FROM enriched
) src
PIVOT (
    MAX(score_adj)
    FOR grade_status IN ('INCOMPLETE', 'FAIL', 'PASS', 'WITHDRAWN')
)
