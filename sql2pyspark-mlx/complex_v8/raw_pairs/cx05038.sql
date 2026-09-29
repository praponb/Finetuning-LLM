WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        LEAD(enrolled_at) OVER (ORDER BY enrolled_at, id) AS next_enrolled_at,
        ROW_NUMBER() OVER (PARTITION BY YEAR(enrolled_at) ORDER BY enrolled_at, id) AS seq_no,
        MAX(score) OVER (PARTITION BY student_id, grade_status) AS student_id_grade_status_max_score,
        COUNT(*) OVER (PARTITION BY TO_DATE(enrolled_at)) AS rows_in_enrolled_at_to_date,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id) AS rn,
        RANK() OVER w_student_id_by_enrolled_at_ti AS enrolled_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(enrolled_at) ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist
    FROM scoped
    WINDOW w_student_id_by_enrolled_at_ti AS (PARTITION BY student_id ORDER BY enrolled_at DESC)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM sequenced
    WHERE enrolled_at_rank <= 3
),
per_group AS (
    -- Summarise per group
    SELECT
        rn,
        enrolled_at_rank,
        COUNT(*) AS row_count,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        COUNT(DISTINCT id) AS distinct_ids
    FROM top_per_group
    WHERE seq_no > 2
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
metrics AS (
    -- Previous / next values per key
    SELECT
        rn,
        enrolled_at_rank,
        row_count,
        distinct_ids,
        MIN(enrolled_at_rank) OVER (PARTITION BY distinct_course_ids, row_count ORDER BY distinct_course_ids ASC, rn, enrolled_at_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_enrolled_at_rank_so_far,
        DENSE_RANK() OVER (PARTITION BY distinct_course_ids, row_count ORDER BY distinct_course_ids ASC) AS distinct_course_ids_dense_rank,
        SUM(enrolled_at_rank) OVER (PARTITION BY distinct_course_ids, row_count ORDER BY distinct_course_ids ASC, rn, enrolled_at_rank ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS enrolled_at_rank_sum_4,
        ROUND(row_count - AVG(row_count) OVER w_row_count, 2) AS row_count_vs_row_count_avg
    FROM per_group
    WINDOW w_row_count AS (PARTITION BY row_count)
)
-- Final projection
SELECT
    rn,
    enrolled_at_rank,
    row_count,
    distinct_ids,
    min_enrolled_at_rank_so_far,
    enrolled_at_rank_sum_4,
    row_count_vs_row_count_avg,
    COALESCE(distinct_ids, 0) AS distinct_ids_filled,
    COALESCE(enrolled_at_rank_sum_4, 0) AS enrolled_at_rank_sum_4_filled
FROM metrics
WHERE row_count < 10
ORDER BY min_enrolled_at_rank_so_far
