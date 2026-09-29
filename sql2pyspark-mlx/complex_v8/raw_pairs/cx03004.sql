WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE term IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        term,
        score,
        enrolled_at,
        term_ref,
        score - LAG(score) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS score_change,
        ROUND(score / NULLIF(SUM(score) OVER w_term_enrolled_at_year, 0), 4) AS score_share_of_term_enrolled_at_year,
        LEAD(score, 3) OVER (PARTITION BY term, course_id ORDER BY enrolled_at DESC, id) AS next3_score,
        ROUND(CUME_DIST() OVER w_term_course_id_by_enrolled_a_2, 3) AS enrolled_at_cume_dist,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        ROUND(AVG(score) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS score_avg_4,
        MAX(score) OVER (PARTITION BY term, YEAR(enrolled_at)) AS term_enrolled_at_year_max_score,
        SUM(score) OVER (PARTITION BY term, course_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score
    FROM filtered
    WINDOW w_term_enrolled_at_year AS (PARTITION BY term, YEAR(enrolled_at)), w_term_course_id_by_enrolled_a_2 AS (PARTITION BY term, course_id ORDER BY enrolled_at DESC)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        enrolled_at_cume_dist + score_share_of_course_id AS enrolled_at_cume_dist_plus_score_share_of_course,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM with_windows
)
-- Final projection
SELECT
    id,
    term,
    score,
    enrolled_at,
    term_ref,
    score_change,
    score_share_of_term_enrolled_at_year,
    score_avg_4,
    term_enrolled_at_year_max_score,
    running_score,
    enrolled_at_cume_dist_plus_score_share_of_course,
    term_or_unknown
FROM scored
ORDER BY enrolled_at DESC
