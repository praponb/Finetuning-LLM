WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CASE WHEN grade_status IN ('INCOMPLETE', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        ABS(score - 57.3) AS score_distance
    FROM enrollments
    WHERE grade_status = 'PASS'
      AND student_id IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(term), 1, 4) AS term_prefix
    FROM scoped
    WHERE score_distance BETWEEN 20.0 AND 65.0
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status = 'FAIL'
              AND e.term IN ('2024T1', '2024T2', '2025T2')
        )
      AND s.term_prefix IS NOT NULL
),
grade_status_group_stats AS (
    -- Avg score per grade_status_group
    SELECT grade_status_group, AVG(score) AS avg_score
    FROM filtered
    GROUP BY grade_status_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM filtered s
    JOIN grade_status_group_stats g ON g.grade_status_group = s.grade_status_group
    WHERE s.score > g.avg_score
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT term, enrolled_at, grade_status_group, avg_score
    FROM vs_group
) src
PIVOT (
    SUM(avg_score)
    FOR grade_status_group IN ('group_b', 'other')
)
ORDER BY term, enrolled_at
