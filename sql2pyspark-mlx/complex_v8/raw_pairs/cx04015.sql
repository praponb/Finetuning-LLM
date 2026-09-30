WITH recent_enrollments AS (
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
    WHERE course_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(term) OVER w_grade_status_student_id) AS distinct_term_in_grade_status_student_id,
        MIN(score) OVER (PARTITION BY grade_status, student_id) AS grade_status_student_id_min_score,
        ROW_NUMBER() OVER w_all_by_score AS rn,
        ROUND(CUME_DIST() OVER w_all_by_score_ties, 3) AS score_cume_dist,
        score - LAG(score) OVER (PARTITION BY term ORDER BY score ASC, id) AS score_change,
        MAX(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_max_1_prior,
        LAG(score, 1, 0) OVER (PARTITION BY student_id, term ORDER BY enrolled_at DESC, id) AS prev_score,
        FIRST_VALUE(score) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score
    FROM recent_enrollments
    WINDOW w_grade_status_student_id AS (PARTITION BY grade_status, student_id), w_all_by_score AS (ORDER BY score, id), w_all_by_score_ties AS (ORDER BY score)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    distinct_term_in_grade_status_student_id,
    grade_status_student_id_min_score,
    score_cume_dist,
    score_max_1_prior,
    prev_score,
    DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
    ABS(score - 34.2) AS score_distance
FROM windowed
ORDER BY days_since_enrolled_at NULLS LAST, term, id
LIMIT 10
