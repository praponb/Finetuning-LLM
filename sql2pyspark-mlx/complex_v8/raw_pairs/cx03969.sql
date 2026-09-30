WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT *
    FROM enrollments
    WHERE grade_status IS NOT NULL
      AND course_id >= 9
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        term,
        grade_status,
        enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id ORDER BY score DESC), 3) AS score_percent_rank,
        LEAD(enrolled_at) OVER (ORDER BY enrolled_at DESC, id) AS next_enrolled_at,
        LAST_VALUE(score) OVER (PARTITION BY YEAR(enrolled_at), course_id ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        DENSE_RANK() OVER w_enrolled_at_year_course_id_b_2 AS score_dense_rank,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY grade_status)) AS distinct_term_in_grade_status,
        COUNT(*) OVER (PARTITION BY grade_status, course_id) AS rows_in_grade_status_course_id,
        FIRST_VALUE(score) OVER (PARTITION BY YEAR(enrolled_at), course_id ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        MAX(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_max_7,
        LEAD(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS next_enrolled_at_2,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY YEAR(enrolled_at), course_id ORDER BY score ASC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROW_NUMBER() OVER (ORDER BY enrolled_at DESC, id) AS seq_no,
        MIN(score) OVER (PARTITION BY student_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        MIN(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far_2,
        score - LAG(score) OVER w_term_by_enrolled_at AS score_change
    FROM cleaned_enrollments
    WINDOW w_enrolled_at_year_course_id_b_2 AS (PARTITION BY YEAR(enrolled_at), course_id ORDER BY score ASC, id), w_term_by_enrolled_at AS (PARTITION BY term ORDER BY enrolled_at, id)
),
per_group AS (
    -- Group-level totals
    SELECT
        term,
        YEAR(enrolled_at) AS enrolled_at_year,
        COUNT(DISTINCT grade_status) AS distinct_grade_status,
        SUM(CASE WHEN term IN ('2025T2', '2025T1') THEN distinct_term_in_grade_status ELSE 0 END) AS distinct_term_in_grade_status_term_in_2025t2_202,
        ROUND(AVG(minutes_since_prev_enrolled_at), 2) AS avg_minutes_since_prev_enrolled_at
    FROM metrics
    GROUP BY term, YEAR(enrolled_at)
    HAVING MAX(score_max_7) > 75.0
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY term, enrolled_at_year ORDER BY avg_minutes_since_prev_enrolled_at DESC) AS avg_minutes_since_prev_enrolled_at_dense_rank,
        ROUND(distinct_grade_status - AVG(distinct_grade_status) OVER (PARTITION BY term), 2) AS distinct_grade_status_vs_term_avg
    FROM per_group
    WINDOW w_term_enrolled_at_year_by_avg AS (PARTITION BY term, enrolled_at_year ORDER BY avg_minutes_since_prev_enrolled_at DESC)
)
SELECT
    term,
    enrolled_at_year,
    distinct_grade_status,
    distinct_term_in_grade_status_term_in_2025t2_202,
    avg_minutes_since_prev_enrolled_at,
    avg_minutes_since_prev_enrolled_at_dense_rank,
    distinct_grade_status_vs_term_avg
FROM with_windows
ORDER BY avg_minutes_since_prev_enrolled_at_dense_rank DESC, avg_minutes_since_prev_enrolled_at
