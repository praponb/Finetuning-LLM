WITH prepared AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        credits,
        level
    FROM courses
    WHERE credits > 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        credits,
        ROUND(CUME_DIST() OVER (PARTITION BY credits ORDER BY credits, id), 3) AS credits_cume_dist,
        SUM(credits) OVER (PARTITION BY level, credits) AS level_credits_sum_credits,
        DENSE_RANK() OVER (PARTITION BY credits ORDER BY credits, id) AS credits_dense_rank,
        MIN(credits) OVER (PARTITION BY level, credits) AS level_credits_min_credits,
        ROW_NUMBER() OVER (PARTITION BY credits ORDER BY credits, id) AS seq_no,
        COUNT(*) OVER w_level_credits AS rows_in_level_credits
    FROM prepared
    WINDOW w_level_credits AS (PARTITION BY level, credits)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(credits) OVER w_credits_dense_rank_credits AS credits_dense_rank_credits_sum_credits,
        MIN(credits) OVER w_credits_dense_rank_credits AS credits_dense_rank_credits_min_credits
    FROM with_windows
    WINDOW w_credits_dense_rank_credits AS (PARTITION BY credits_dense_rank, credits)
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.term IN ('2024T1', '2024T2', '2025T1')
              AND e.grade_status IN ('FAIL', 'PASS')
        )
)
SELECT
    credits,
    seq_no,
    COUNT(*) AS n_rows,
    PERCENTILE_APPROX(credits_cume_dist, 0.5) AS median_credits_cume_dist
FROM without_matches
GROUP BY CUBE(credits, seq_no)
