WITH prepared AS (
    -- Keep valid rows and derive helper columns
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
    WHERE course_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY course_id, student_id) AS rows_in_course_id_student_id,
        ROUND(PERCENT_RANK() OVER w_grade_status_student_id_by_e, 3) AS enrolled_at_percent_rank,
        ROUND(PERCENT_RANK() OVER w_term_by_enrolled_at, 3) AS enrolled_at_percent_rank_2,
        NTH_VALUE(score, 2) OVER (PARTITION BY term ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        LAST_VALUE(enrolled_at) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_avg_2_prior,
        FIRST_VALUE(score) OVER (PARTITION BY term ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        RANK() OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC) AS enrolled_at_rank,
        NTILE(4) OVER w_student_id_by_enrolled_at AS enrolled_at_quartile,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_cume_dist,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior,
        NTILE(10) OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC, id) AS enrolled_at_decile,
        ROUND(AVG(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        SUM(score) OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score
    FROM prepared
    WINDOW w_grade_status_student_id_by_e AS (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC), w_term_by_enrolled_at AS (PARTITION BY term ORDER BY enrolled_at DESC, id), w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at DESC, id)
),
scored AS (
    -- Derive helper columns
    SELECT *, FLOOR(second_score / 25) * 25 AS second_score_bucket
    FROM windowed
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        term,
        score,
        enrolled_at,
        term_or_unknown,
        rows_in_course_id_student_id,
        second_score,
        last_enrolled_at,
        first_score,
        enrolled_at_rank,
        score_share_of_course_id,
        enrolled_at_cume_dist,
        score_avg_6_prior,
        enrolled_at_decile,
        score_avg_3_prior,
        running_score,
        second_score_bucket,
        RANK() OVER w_all_by_enrolled_at AS enrolled_at_rank_2,
        ROW_NUMBER() OVER (ORDER BY enrolled_at_percent_rank_2 ASC, id) AS seq_no,
        LAST_VALUE(running_score) OVER (ORDER BY running_score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_running_score,
        ROUND(score_avg_2_prior - AVG(score_avg_2_prior) OVER w_term_or_unknown_enrolled_at, 2) AS score_avg_2_prior_vs_term_or_unknown_enrolled_at
    FROM scored
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC, id), w_term_or_unknown_enrolled_at AS (PARTITION BY term_or_unknown, enrolled_at_quartile)
)
SELECT
    id,
    student_id,
    term,
    enrolled_at,
    rows_in_course_id_student_id,
    last_enrolled_at,
    enrolled_at_rank,
    score_share_of_course_id,
    score_avg_6_prior,
    score_avg_3_prior,
    running_score,
    second_score_bucket,
    enrolled_at_rank_2,
    last_running_score,
    score_avg_2_prior_vs_term_or_unknown_enrolled_at,
    CASE WHEN score >= 59.8 THEN 'HIGH' WHEN score >= 26.2 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
    COALESCE(enrolled_at_rank_2, 0) AS enrolled_at_rank_2_filled
FROM sequenced
