WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status = 'FAIL'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        score - LAG(score) OVER (ORDER BY enrolled_at DESC, id) AS score_change,
        SIZE(COLLECT_SET(grade_status) OVER w_student_id_course_id) AS distinct_grade_status_in_student_id_course_id,
        ROUND(score - AVG(score) OVER w_course_id, 2) AS score_vs_course_id_avg,
        NTILE(10) OVER (PARTITION BY YEAR(enrolled_at) ORDER BY enrolled_at DESC, id) AS enrolled_at_decile,
        ROUND(AVG(score) OVER (PARTITION BY student_id ORDER BY score, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_avg_2_prior,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY enrolled_at DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        SUM(score) OVER (PARTITION BY YEAR(enrolled_at) ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        MIN(score) OVER (PARTITION BY student_id, course_id) AS student_id_course_id_min_score,
        LEAD(enrolled_at) OVER (PARTITION BY YEAR(enrolled_at) ORDER BY enrolled_at DESC, id) AS next_enrolled_at,
        ROUND(PERCENT_RANK() OVER w_student_id_by_score, 3) AS score_percent_rank,
        SUM(score) OVER (PARTITION BY course_id) AS course_id_sum_score
    FROM valid
    WINDOW w_student_id_course_id AS (PARTITION BY student_id, course_id), w_course_id AS (PARTITION BY course_id), w_student_id_by_score AS (PARTITION BY student_id ORDER BY score, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN score_avg_2_prior >= 55.0 THEN 'A' WHEN score_avg_2_prior >= 20.0 THEN 'B' ELSE 'C' END AS score_avg_2_prior_tier,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at
    FROM with_windows
    WHERE minutes_since_prev_enrolled_at BETWEEN 1000.0 AND 4000.0
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    last_known_enrolled_at,
    score_change,
    distinct_grade_status_in_student_id_course_id,
    enrolled_at_decile,
    score_avg_2_prior,
    minutes_since_prev_enrolled_at,
    running_score,
    student_id_course_id_min_score,
    course_id_sum_score,
    score_avg_2_prior_tier,
    days_since_enrolled_at,
    ABS(score_change - 10.0) AS score_change_distance,
    GREATEST(enrolled_at_decile, score_avg_2_prior) AS greatest_enrolled_at_decile_score_avg_2_prior
FROM tagged
WHERE distinct_grade_status_in_student_id_course_id <= 9
ORDER BY score_avg_2_prior
