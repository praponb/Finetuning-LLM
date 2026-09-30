WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(score / NULLIF(SUM(score) OVER w_grade_status, 0), 4) AS score_share_of_grade_status,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank,
        ROUND(score - AVG(score) OVER (PARTITION BY term), 2) AS score_vs_term_avg,
        SUM(score) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_sum_4,
        ROUND(AVG(score) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_avg_1_prior,
        LAG(enrolled_at) OVER w_enrolled_at_year_term_by_sco AS prev_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id ORDER BY score), 3) AS score_percent_rank,
        LEAD(score, 3, 0) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id) AS next3_score,
        FIRST_VALUE(score) OVER (PARTITION BY course_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score
    FROM recent
    WINDOW w_grade_status AS (PARTITION BY grade_status), w_enrolled_at_year_term_by_sco AS (PARTITION BY YEAR(enrolled_at), term ORDER BY score ASC, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        score,
        term_or_unknown,
        score_share_of_grade_status,
        enrolled_at_percent_rank,
        score_sum_4,
        score_avg_1_prior,
        prev_enrolled_at,
        next3_score,
        first_score,
        SIZE(COLLECT_SET(term_or_unknown) OVER w_grade_status) AS distinct_term_or_unknown_in_grade_status,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY grade_status, DATE_TRUNC('MONTH', prev_enrolled_at))) AS distinct_course_id_in_grade_status_prev_enrolled,
        FIRST_VALUE(score_vs_term_avg) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), grade_status ORDER BY next3_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score_vs_term_avg,
        ROUND(AVG(score_avg_1_prior) OVER (PARTITION BY grade_status, DATE_TRUNC('MONTH', prev_enrolled_at)), 2) AS grade_status_prev_enrolled_at_month_avg_score_av
    FROM windowed
    WINDOW w_grade_status AS (PARTITION BY grade_status)
)
SELECT
    id,
    student_id,
    term,
    score,
    term_or_unknown,
    score_share_of_grade_status,
    score_avg_1_prior,
    next3_score,
    first_score,
    distinct_course_id_in_grade_status_prev_enrolled,
    first_score_vs_term_avg,
    grade_status_prev_enrolled_at_month_avg_score_av,
    COALESCE(score, 0.0) AS score_filled,
    CONCAT_WS(' / ', term, term_or_unknown) AS term_term_or_unknown_label
FROM ranked
ORDER BY first_score_vs_term_avg DESC NULLS FIRST, first_score, id
LIMIT 10
