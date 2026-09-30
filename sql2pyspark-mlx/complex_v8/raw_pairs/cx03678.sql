WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('WITHDRAWN', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE course_id <= 6
      AND grade_status NOT IN ('FAIL', 'WITHDRAWN')
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_course_id_grade_status_group AS rows_in_course_id_grade_status_group,
        LAG(enrolled_at, 2) OVER (PARTITION BY student_id ORDER BY score ASC, id) AS prev2_enrolled_at,
        SUM(score) OVER (PARTITION BY student_id ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score
    FROM cleaned_enrollments
    WINDOW w_course_id_grade_status_group AS (PARTITION BY course_id, grade_status_group)
),
term_stats AS (
    -- Avg running_score per term
    SELECT term, AVG(running_score) AS avg_running_score
    FROM with_trends
    GROUP BY term
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_running_score,
        ROUND(s.running_score - g.avg_running_score, 2) AS running_score_minus_avg
    FROM with_trends s
    JOIN term_stats g ON g.term = s.term
    WHERE s.running_score > g.avg_running_score
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
      AND s.term <> '2025T1'
)
-- Subtotals with CUBE
SELECT
    course_id,
    COALESCE(grade_status_group, 'ALL') AS grade_status_group,
    GROUPING_ID() AS gid,
    MAX(running_score_minus_avg) AS max_running_score_minus_avg
FROM screened
GROUP BY CUBE(course_id, grade_status_group)
