WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.score) AS prior_score_1d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(prior_score_1d, 2, 0) OVER (PARTITION BY prior_rows_1d, YEAR(enrolled_at) ORDER BY prior_score_1d DESC, id) AS next2_prior_score_1d,
        LAG(prior_score_1d, 1, 0) OVER w_prior_rows_1d_enrolled_at_ye AS prev_prior_score_1d,
        ROW_NUMBER() OVER (ORDER BY enrolled_at, id) AS seq_no,
        NTH_VALUE(prior_rows_1d, 2) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_rows_1d
    FROM in_window
    WINDOW w_prior_rows_1d_enrolled_at_ye AS (PARTITION BY prior_rows_1d, YEAR(enrolled_at) ORDER BY prior_score_1d DESC, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN prior_score_1d >= 300.0 THEN 'A' WHEN prior_score_1d >= 200.0 THEN 'B' ELSE 'C' END AS prior_score_1d_tier,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_ym,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(second_prior_rows_1d), 2) AS avg_second_prior_rows_1d,
        PERCENTILE_APPROX(seq_no, 0.5) AS median_seq_no,
        COUNT(*) AS cnt
    FROM with_trends
    GROUP BY CASE WHEN prior_score_1d >= 300.0 THEN 'A' WHEN prior_score_1d >= 200.0 THEN 'B' ELSE 'C' END, DATE_FORMAT(enrolled_at, 'yyyy-MM')
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_ids, prior_score_1d_tier, cnt
    FROM stats
) src
PIVOT (
    SUM(cnt)
    FOR prior_score_1d_tier IN ('C', 'A')
)
ORDER BY distinct_ids
