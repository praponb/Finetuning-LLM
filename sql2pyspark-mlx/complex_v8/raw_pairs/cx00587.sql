WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE score > 17.4
      AND course_id IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROW_NUMBER() OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC, id) AS enrolled_at_seq,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status, student_id), 2) AS score_vs_grade_status_student_id_avg,
        SIZE(COLLECT_SET(course_id) OVER w_grade_status_student_id) AS distinct_course_id_in_grade_status_student_id,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id, grade_status ORDER BY score, id), 3) AS score_cume_dist,
        COUNT(*) OVER w_grade_status_student_id AS rows_in_grade_status_student_id,
        LEAD(score, 2) OVER w_course_id_grade_status_by_sc AS next2_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status, student_id), 0), 4) AS score_share_of_grade_status_student_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        LAG(score) OVER w_course_id_grade_status_by_en_3 AS prev_score
    FROM cleaned_enrollments
    WINDOW w_grade_status_student_id AS (PARTITION BY grade_status, student_id), w_course_id_grade_status_by_sc AS (PARTITION BY course_id, grade_status ORDER BY score, id), w_course_id_grade_status_by_en_3 AS (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id)
),
tagged AS (
    -- Label and score rows
    SELECT *, ROUND(prev_score * 1.5, 2) AS prev_score_adj
    FROM ranked
)
-- Final projection
SELECT
    id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    first_known_enrolled_at,
    score_vs_grade_status_student_id_avg,
    distinct_course_id_in_grade_status_student_id,
    rows_in_grade_status_student_id,
    next2_score,
    score_share_of_grade_status_student_id,
    enrolled_at_percent_rank,
    prev_score,
    prev_score_adj
FROM tagged
