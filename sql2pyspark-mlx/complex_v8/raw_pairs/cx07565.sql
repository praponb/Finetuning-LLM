WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE term NOT IN ('2024T2', '2025T2')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        score,
        grade_status,
        enrolled_at,
        grade_status_len,
        score_filled,
        ROUND(PERCENT_RANK() OVER (ORDER BY grade_status_len), 3) AS grade_status_len_percent_rank,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY score_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        score - LAG(score) OVER (PARTITION BY grade_status_len, grade_status ORDER BY enrolled_at DESC, id) AS score_change,
        ROUND(CUME_DIST() OVER w_grade_status_len_grade_statu, 3) AS enrolled_at_cume_dist,
        RANK() OVER (ORDER BY score_filled) AS score_filled_rank,
        ROUND(PERCENT_RANK() OVER w_grade_status_len_grade_statu, 3) AS enrolled_at_percent_rank
    FROM valid_enrollments
    WINDOW w_grade_status_len_grade_statu AS (PARTITION BY grade_status_len, grade_status ORDER BY enrolled_at DESC, id)
),
grade_status_stats AS (
    -- Avg score_filled per grade_status
    SELECT grade_status, AVG(score_filled) AS avg_score_filled
    FROM with_trends
    GROUP BY grade_status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_score_filled,
        ROUND(s.score_filled - g.avg_score_filled, 2) AS score_filled_minus_avg
    FROM with_trends s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score_filled > g.avg_score_filled
)
SELECT id, measure, metric_value
FROM (
    SELECT id, avg_score_filled, score_filled, score
    FROM vs_group
) src
UNPIVOT (
    metric_value FOR measure IN (avg_score_filled, score_filled, score)
) unp
ORDER BY id, measure
