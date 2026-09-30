WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE term = '2025T1'
      AND score < 66.0
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        course_id,
        term,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM scoped
    WHERE course_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY term ORDER BY enrolled_at) AS enrolled_at_dense_rank
    FROM derived
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        enrolled_at_dense_rank,
        DATE_TRUNC('MONTH', first_known_enrolled_at) AS first_known_enrolled_at_month,
        DATE_TRUNC('MONTH', last_enrolled_at) AS last_enrolled_at_month,
        ROUND(AVG(enrolled_at_dense_rank), 2) AS avg_enrolled_at_dense_rank,
        COUNT(*) AS row_count,
        SUM(enrolled_at_dense_rank) AS total_enrolled_at_dense_rank
    FROM sequenced
    WHERE enrolled_at_dense_rank > 2
    GROUP BY enrolled_at_dense_rank, DATE_TRUNC('MONTH', first_known_enrolled_at), DATE_TRUNC('MONTH', last_enrolled_at)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        enrolled_at_dense_rank,
        first_known_enrolled_at_month,
        last_enrolled_at_month,
        avg_enrolled_at_dense_rank,
        ROUND(total_enrolled_at_dense_rank - AVG(total_enrolled_at_dense_rank) OVER w_row_count_enrolled_at_dense, 2) AS total_enrolled_at_dense_rank_vs_row_count_enroll,
        DENSE_RANK() OVER (PARTITION BY enrolled_at_dense_rank, row_count ORDER BY total_enrolled_at_dense_rank DESC, enrolled_at_dense_rank, first_known_enrolled_at_month, last_enrolled_at_month) AS total_enrolled_at_dense_rank_dense_rank,
        ROUND(AVG(total_enrolled_at_dense_rank) OVER (PARTITION BY row_count, enrolled_at_dense_rank), 2) AS row_count_enrolled_at_dense_rank_avg_total_enrol,
        MAX(avg_enrolled_at_dense_rank) OVER (PARTITION BY row_count, enrolled_at_dense_rank) AS row_count_enrolled_at_dense_rank_max_avg_enrolle,
        MAX(avg_enrolled_at_dense_rank) OVER (PARTITION BY row_count, enrolled_at_dense_rank ORDER BY total_enrolled_at_dense_rank NULLS LAST, enrolled_at_dense_rank, first_known_enrolled_at_month, last_enrolled_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_avg_enrolled_at_dense_rank_so_far
    FROM totals
    WINDOW w_row_count_enrolled_at_dense AS (PARTITION BY row_count, enrolled_at_dense_rank)
)
SELECT
    enrolled_at_dense_rank,
    first_known_enrolled_at_month,
    last_enrolled_at_month,
    total_enrolled_at_dense_rank_vs_row_count_enroll,
    total_enrolled_at_dense_rank_dense_rank,
    row_count_enrolled_at_dense_rank_avg_total_enrol,
    MONTH(last_enrolled_at_month) AS last_enrolled_at_month_month,
    ABS(enrolled_at_dense_rank - 6) AS enrolled_at_dense_rank_distance
FROM with_windows
WHERE first_known_enrolled_at_month < TIMESTAMP '2025-02-17 09:00:00'
ORDER BY last_enrolled_at_month DESC NULLS FIRST, enrolled_at_dense_rank
