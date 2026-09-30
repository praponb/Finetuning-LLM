WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at_2,
        ROUND(score * 1.5, 2) AS score_adj
    FROM enrollments
    WHERE score < 63.9
      AND term = '2025T1'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        grade_status,
        enrolled_at,
        days_since_enrolled_at_2,
        score_adj,
        ROUND(PERCENT_RANK() OVER w_enrolled_at_month_grade_stat, 3) AS enrolled_at_percent_rank,
        SUM(score_adj) OVER (PARTITION BY student_id ORDER BY score_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_adj,
        ROW_NUMBER() OVER (ORDER BY days_since_enrolled_at, id) AS days_since_enrolled_at_seq,
        ROUND(AVG(score_adj) OVER (ORDER BY days_since_enrolled_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_adj_avg_2_prior,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY grade_status)) AS distinct_student_id_in_grade_status
    FROM valid
    WINDOW w_enrolled_at_month_grade_stat AS (PARTITION BY MONTH(enrolled_at), grade_status ORDER BY enrolled_at)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score
    FROM sequenced s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, score_adj, score_adj_avg_2_prior, running_score_adj
    FROM with_stats
) src
UNPIVOT (
    value FOR metric IN (score_adj, score_adj_avg_2_prior, running_score_adj)
) unp
