WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        cohort,
        major,
        is_international,
        ADD_MONTHS(enrolled_on, 12) AS enrolled_on_plus_12m
    FROM students
    WHERE cohort NOT IN ('C2022', 'C2024')
      AND major = 'CS'
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        cohort,
        major,
        enrolled_on_plus_12m,
        SUBSTRING(TRIM(cohort), 1, 4) AS cohort_prefix,
        DAYOFWEEK(enrolled_on_plus_12m) AS enrolled_on_plus_12m_dayofweek
    FROM prepared
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        cohort,
        major,
        enrolled_on_plus_12m,
        LAG(enrolled_on_plus_12m) OVER (PARTITION BY major, enrolled_on_plus_12m_dayofweek ORDER BY enrolled_on_plus_12m_dayofweek ASC NULLS LAST, id) AS prev_enrolled_on_plus_12m,
        LEAD(enrolled_on_plus_12m) OVER (PARTITION BY major, enrolled_on_plus_12m_dayofweek ORDER BY enrolled_on_plus_12m_dayofweek ASC NULLS LAST, id) AS next_enrolled_on_plus_12m,
        RANK() OVER w_major_enrolled_on_plus_12m_d AS enrolled_on_plus_12m_dayofweek_rank
    FROM derived
    WINDOW w_major_enrolled_on_plus_12m_d AS (PARTITION BY major, enrolled_on_plus_12m_dayofweek ORDER BY enrolled_on_plus_12m_dayofweek ASC NULLS LAST, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        cohort,
        prev_enrolled_on_plus_12m,
        next_enrolled_on_plus_12m,
        enrolled_on_plus_12m_dayofweek_rank,
        FIRST_VALUE(enrolled_on_plus_12m) OVER (ORDER BY prev_enrolled_on_plus_12m DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_on_plus_12m,
        DENSE_RANK() OVER w_all_by_prev_enrolled_on_plus_2 AS prev_enrolled_on_plus_12m_dense_rank,
        NTILE(4) OVER (ORDER BY prev_enrolled_on_plus_12m DESC, id) AS prev_enrolled_on_plus_12m_quartile,
        LEAD(next_enrolled_on_plus_12m, 3) OVER (ORDER BY prev_enrolled_on_plus_12m DESC, id) AS next3_next_enrolled_on_plus_12m,
        DENSE_RANK() OVER w_all_by_prev_enrolled_on_plus_3 AS prev_enrolled_on_plus_12m_dense_rank_2
    FROM with_trends
    WINDOW w_all_by_prev_enrolled_on_plus_2 AS (ORDER BY prev_enrolled_on_plus_12m DESC, id), w_all_by_prev_enrolled_on_plus_3 AS (ORDER BY prev_enrolled_on_plus_12m DESC)
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.course_id IS NOT NULL
        )
      AND s.enrolled_on_plus_12m_dayofweek_rank < 8
),
cohort_stats AS (
    -- Avg prev_enrolled_on_plus_12m_dense_rank_2 per cohort
    SELECT cohort, AVG(prev_enrolled_on_plus_12m_dense_rank_2) AS avg_prev_enrolled_on_plus_12m_dense_rank_2
    FROM filtered
    GROUP BY cohort
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prev_enrolled_on_plus_12m_dense_rank_2,
        ROUND(s.prev_enrolled_on_plus_12m_dense_rank_2 - g.avg_prev_enrolled_on_plus_12m_dense_rank_2, 2) AS prev_enrolled_on_plus_12m_dense_rank_2_minus_avg
    FROM filtered s
    JOIN cohort_stats g ON g.cohort = s.cohort
    WHERE s.prev_enrolled_on_plus_12m_dense_rank_2 > g.avg_prev_enrolled_on_plus_12m_dense_rank_2
),
filtered_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM vs_group
    WHERE avg_prev_enrolled_on_plus_12m_dense_rank_2 < (SELECT MAX(avg_prev_enrolled_on_plus_12m_dense_rank_2) FROM vs_group)
)
SELECT id, measure, amount
FROM (
    SELECT id, avg_prev_enrolled_on_plus_12m_dense_rank_2, prev_enrolled_on_plus_12m_dense_rank_2_minus_avg
    FROM filtered_2
) src
UNPIVOT (
    amount FOR measure IN (avg_prev_enrolled_on_plus_12m_dense_rank_2, prev_enrolled_on_plus_12m_dense_rank_2_minus_avg)
) unp
