WITH valid_enrollments AS (
    -- Scope the enrollments rows
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
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        COUNT(*) OVER w_grade_status AS rows_in_grade_status,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY score, id) AS score_tercile,
        SUM(score) OVER (PARTITION BY grade_status, course_id ORDER BY score DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_sum_3,
        NTILE(3) OVER (ORDER BY score, id) AS score_tercile_2,
        LAG(enrolled_at) OVER (PARTITION BY grade_status, course_id ORDER BY score DESC, id) AS prev_enrolled_at,
        LEAD(enrolled_at) OVER (PARTITION BY YEAR(enrolled_at), term ORDER BY score ASC, id) AS next_enrolled_at,
        ROW_NUMBER() OVER (PARTITION BY YEAR(enrolled_at), term ORDER BY score ASC, id) AS score_seq,
        RANK() OVER (PARTITION BY grade_status, course_id ORDER BY score DESC, id) AS score_rank,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status, term), 2) AS score_vs_grade_status_term_avg,
        MIN(score) OVER w_grade_status_term AS grade_status_term_min_score
    FROM valid_enrollments
    WINDOW w_grade_status AS (PARTITION BY grade_status), w_grade_status_term AS (PARTITION BY grade_status, term)
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        rows_in_grade_status,
        score_tercile,
        score_sum_3,
        score_tercile_2,
        prev_enrolled_at,
        next_enrolled_at,
        grade_status_term_min_score,
        grade_status_term_min_score - LAG(grade_status_term_min_score) OVER (PARTITION BY grade_status ORDER BY score_rank ASC NULLS LAST, id) AS grade_status_term_min_score_change,
        ROUND(AVG(grade_status_term_min_score) OVER (ORDER BY prev_enrolled_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS grade_status_term_min_score_avg_4,
        FIRST_VALUE(next_enrolled_at) IGNORE NULLS OVER (ORDER BY prev_enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_enrolled_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY score_vs_grade_status_term_avg), 3) AS score_vs_grade_status_term_avg_percent_rank
    FROM metrics
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    grade_status,
    rows_in_grade_status,
    score_tercile,
    score_sum_3,
    score_tercile_2,
    grade_status_term_min_score_change,
    grade_status_term_min_score_avg_4,
    score_vs_grade_status_term_avg_percent_rank,
    ABS(rows_in_grade_status - 10) AS rows_in_grade_status_distance,
    GREATEST(score_tercile, score) AS greatest_score_tercile_score
FROM metrics_2
ORDER BY greatest_score_tercile_score DESC, id
LIMIT 20
