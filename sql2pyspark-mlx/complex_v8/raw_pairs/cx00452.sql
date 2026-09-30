WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('INCOMPLETE', 'FAIL') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        enrolled_at + INTERVAL 7 DAYS AS enrolled_at_plus_7d
    FROM enrollments
    WHERE (term NOT IN ('2024T2', '2024T1') OR grade_status <> 'FAIL')
),
student_id_stats AS (
    -- Avg score per student_id
    SELECT student_id, AVG(score) AS avg_score
    FROM filtered_enrollments
    GROUP BY student_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM filtered_enrollments s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score > g.avg_score
),
grade_status_group_stats AS (
    -- Median score_minus_avg per grade_status_group
    SELECT grade_status_group, PERCENTILE_APPROX(score_minus_avg, 0.5) AS median_score_minus_avg
    FROM compared
    GROUP BY grade_status_group
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score_minus_avg,
        ROUND(s.score_minus_avg - g.median_score_minus_avg, 2) AS score_minus_avg_minus_median
    FROM compared s
    JOIN grade_status_group_stats g ON g.grade_status_group = s.grade_status_group
    WHERE s.score_minus_avg >= g.median_score_minus_avg
)
SELECT *
FROM (
    SELECT enrolled_at_plus_7d, term, grade_status_group, score_minus_avg_minus_median
    FROM above_average
) src
PIVOT (
    SUM(score_minus_avg_minus_median)
    FOR grade_status_group IN ('group_b', 'other', 'group_a')
)
ORDER BY enrolled_at_plus_7d, term
