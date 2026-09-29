WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE course_id >= 3
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.major AS student_major,
        st.enrolled_on AS student_enrolled_on,
        st.is_international AS student_is_international
    FROM valid_enrollments s
    JOIN students st ON st.id = s.student_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        student_major,
        student_is_international,
        DENSE_RANK() OVER (ORDER BY score, id) AS score_dense_rank,
        LAG(student_enrolled_on, 3) OVER (PARTITION BY student_id ORDER BY score DESC, id) AS prev3_student_enrolled_on,
        SUM(score) OVER (PARTITION BY student_major, student_is_international) AS student_major_student_is_international_sum_score,
        MIN(score) OVER (PARTITION BY student_id, YEAR(student_enrolled_on)) AS student_id_student_enrolled_on_year_min_score,
        NTILE(3) OVER (PARTITION BY student_is_international ORDER BY enrolled_at ASC, id) AS enrolled_at_tercile,
        LAG(score, 2) OVER w_student_enrolled_on_month_by AS prev2_score,
        DATEDIFF(student_enrolled_on, LAG(student_enrolled_on) OVER (ORDER BY score, id)) AS days_since_prev_student_enrolled_on,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY student_major, student_is_international)) AS distinct_term_in_student_major_student_is_intern,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_all_by_score)) / 60, 1) AS minutes_since_prev_enrolled_at,
        FIRST_VALUE(score) OVER (PARTITION BY student_is_international ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        ROUND(AVG(score) OVER (PARTITION BY student_major, student_is_international), 2) AS student_major_student_is_international_avg_score,
        COUNT(*) OVER w_student_id_student_enrolled AS rows_in_student_id_student_enrolled_on_year,
        NTH_VALUE(score, 2) OVER (PARTITION BY student_is_international ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        SUM(score) OVER (PARTITION BY student_id, YEAR(student_enrolled_on)) AS student_id_student_enrolled_on_year_sum_score
    FROM combined_details
    WINDOW w_student_enrolled_on_month_by AS (PARTITION BY MONTH(student_enrolled_on) ORDER BY enrolled_at NULLS LAST, id), w_all_by_score AS (ORDER BY score, id), w_student_id_student_enrolled AS (PARTITION BY student_id, YEAR(student_enrolled_on))
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(score_dense_rank) OVER (PARTITION BY enrolled_at_tercile) AS enrolled_at_tercile_sum_score_dense_rank,
        SUM(enrolled_at_tercile) OVER (ORDER BY prev2_score, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS enrolled_at_tercile_sum_3,
        COUNT(*) OVER w_enrolled_at_tercile AS rows_in_enrolled_at_tercile,
        LEAD(prev3_student_enrolled_on, 2) OVER w_prev3_student_enrolled_on_mo AS next2_prev3_student_enrolled_on,
        MIN(score_dense_rank) OVER (PARTITION BY enrolled_at_tercile) AS enrolled_at_tercile_min_score_dense_rank,
        ROUND(CUME_DIST() OVER w_all_by_prev2_score_ties, 3) AS prev2_score_cume_dist,
        ROUND(enrolled_at_tercile - AVG(enrolled_at_tercile) OVER (PARTITION BY enrolled_at_tercile), 2) AS enrolled_at_tercile_vs_enrolled_at_tercile_avg
    FROM with_trends
    WINDOW w_enrolled_at_tercile AS (PARTITION BY enrolled_at_tercile), w_prev3_student_enrolled_on_mo AS (PARTITION BY DATE_TRUNC('MONTH', prev3_student_enrolled_on) ORDER BY rows_in_student_id_student_enrolled_on_year DESC NULLS FIRST, id), w_all_by_prev2_score_ties AS (ORDER BY prev2_score)
),
grouped AS (
    -- Group-level totals
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE student_is_international = TRUE) AS n_student_is_international_tru,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(prev2_score), 2) AS stddev_prev2_score,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        MAX(next2_prev3_student_enrolled_on) AS last_next2_prev3_student_enrolled_on
    FROM metrics
    WHERE student_id_student_enrolled_on_year_min_score BETWEEN 200.0 AND 600.0
    GROUP BY 1
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    course_id,
    n_student_is_international_tru,
    n_rows,
    stddev_prev2_score,
    last_next2_prev3_student_enrolled_on
FROM grouped
ORDER BY n_rows
