WITH scoped_enrollments AS (
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
    WHERE grade_status NOT IN ('PASS', 'INCOMPLETE')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        grade_status,
        enrolled_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY score DESC, id), 3) AS score_percent_rank,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_id, term)) AS distinct_student_id_in_course_id_term,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id, term), 0), 4) AS score_share_of_course_id_term,
        ROUND(PERCENT_RANK() OVER (ORDER BY score DESC), 3) AS score_percent_rank_2,
        FIRST_VALUE(score) OVER (PARTITION BY MONTH(enrolled_at) ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY MONTH(enrolled_at) ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        SUM(score) OVER (PARTITION BY course_id, term) AS course_id_term_sum_score,
        NTILE(4) OVER w_all_by_score AS score_quartile,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY score DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        MIN(score) OVER w_course_id_term AS course_id_term_min_score,
        ROUND(score / NULLIF(SUM(score) OVER w_grade_status_student_id, 0), 4) AS score_share_of_grade_status_student_id,
        COUNT(*) OVER w_grade_status_student_id AS rows_in_grade_status_student_id
    FROM scoped_enrollments
    WINDOW w_all_by_score AS (ORDER BY score DESC, id), w_course_id_term AS (PARTITION BY course_id, term), w_grade_status_student_id AS (PARTITION BY grade_status, student_id)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(last_known_enrolled_at)) / 60, 1) AS minutes_last_known_enrolled_at_to_enrolled_at
    FROM windowed
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        grade_status,
        enrolled_at,
        first_known_score,
        distinct_student_id_in_course_id_term,
        score_share_of_course_id_term,
        score_percent_rank_2,
        first_score,
        last_known_enrolled_at,
        course_id_term_sum_score,
        score_quartile,
        course_id_term_min_score,
        score_share_of_grade_status_student_id,
        rows_in_grade_status_student_id,
        minutes_last_known_enrolled_at_to_enrolled_at,
        ROUND((UNIX_TIMESTAMP(last_known_enrolled_at) - UNIX_TIMESTAMP(LAG(last_known_enrolled_at) OVER (PARTITION BY score_quartile ORDER BY score_share_of_course_id_term, id))) / 60, 1) AS minutes_since_prev_last_known_enrolled_at,
        LAG(first_known_score, 1, 0) OVER (ORDER BY minutes_since_prev_enrolled_at ASC, id) AS prev_first_known_score,
        LEAD(score_percent_rank) OVER w_all_by_minutes_since_prev_en AS next_score_percent_rank,
        COUNT(*) OVER (PARTITION BY YEAR(last_known_enrolled_at), student_id) AS rows_in_last_known_enrolled_at_year_student_id,
        ROUND(score_percent_rank / NULLIF(SUM(score_percent_rank) OVER (PARTITION BY YEAR(last_known_enrolled_at), student_id), 0), 4) AS score_percent_rank_share_of_last_known_enrolled_,
        ROW_NUMBER() OVER (ORDER BY minutes_since_prev_enrolled_at ASC, id) AS rn
    FROM scored
    WINDOW w_all_by_minutes_since_prev_en AS (ORDER BY minutes_since_prev_enrolled_at ASC, id)
)
-- Final projection
SELECT
    id,
    grade_status,
    first_known_score,
    distinct_student_id_in_course_id_term,
    first_score,
    last_known_enrolled_at,
    course_id_term_sum_score,
    score_quartile,
    score_share_of_grade_status_student_id,
    rows_in_grade_status_student_id,
    minutes_last_known_enrolled_at_to_enrolled_at,
    minutes_since_prev_last_known_enrolled_at,
    prev_first_known_score,
    next_score_percent_rank,
    score_percent_rank_share_of_last_known_enrolled_,
    rn,
    COALESCE(rn, 0) AS rn_filled
FROM metrics
ORDER BY rn_filled, prev_first_known_score DESC
