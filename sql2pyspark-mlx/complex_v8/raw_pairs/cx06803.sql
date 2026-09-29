WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.score) AS prior_score_7d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
scored AS (
    -- Add calculated columns
    SELECT *, DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
    FROM in_window
    WHERE prior_score_7d >= 125.0
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        prior_rows_7d,
        student_id,
        ROUND(SUM(prior_rows_7d) / NULLIF(SUM(prior_score_7d), 0), 3) AS prior_rows_7d_to_prior_score_7d_ratio,
        COUNT(*) AS row_count
    FROM scored
    WHERE MONTH(enrolled_at_quarter) IN (2, 3)
    GROUP BY prior_rows_7d, student_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER w_all_by_row_count AS row_count_tercile,
        ROUND(prior_rows_7d - AVG(prior_rows_7d) OVER w_row_count_prior_rows_7d, 2) AS prior_rows_7d_vs_row_count_prior_rows_7d_avg
    FROM rollup_base
    WINDOW w_all_by_row_count AS (ORDER BY row_count DESC NULLS FIRST, prior_rows_7d, student_id), w_row_count_prior_rows_7d AS (PARTITION BY row_count, prior_rows_7d)
)
-- Final projection
SELECT
    prior_rows_7d,
    student_id,
    prior_rows_7d_to_prior_score_7d_ratio,
    row_count,
    prior_rows_7d_vs_row_count_prior_rows_7d_avg,
    ROUND(row_count / NULLIF(row_count_tercile, 0), 3) AS row_count_per_row_count_tercile,
    ROUND(row_count_tercile / NULLIF(row_count, 0), 3) AS row_count_tercile_per_row_count
FROM with_windows
WHERE row_count_tercile > 4
ORDER BY prior_rows_7d_vs_row_count_prior_rows_7d_avg, row_count DESC, prior_rows_7d, student_id
LIMIT 5
