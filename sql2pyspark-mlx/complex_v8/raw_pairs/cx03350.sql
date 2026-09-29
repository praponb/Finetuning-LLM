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
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref
    FROM enrollments
    WHERE score < 57.6
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        score - LAG(score) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id) AS score_change,
        LAG(enrolled_at) OVER w_course_id_by_enrolled_at AS prev_enrolled_at,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status), 2) AS score_vs_grade_status_avg,
        RANK() OVER w_term_grade_status_by_score_t AS score_rank,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY grade_status)) AS distinct_term_in_grade_status,
        RANK() OVER w_student_id_term_by_score_tie AS score_rank_2,
        RANK() OVER (PARTITION BY student_id, term ORDER BY enrolled_at DESC) AS enrolled_at_rank,
        RANK() OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id) AS enrolled_at_rank_2,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id
    FROM prepared
    WINDOW w_course_id_by_enrolled_at AS (PARTITION BY course_id ORDER BY enrolled_at DESC, id), w_term_grade_status_by_score_t AS (PARTITION BY term, grade_status ORDER BY score ASC), w_student_id_term_by_score_tie AS (PARTITION BY student_id, term ORDER BY score)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_change,
        prev_enrolled_at,
        second_enrolled_at,
        score_vs_grade_status_avg,
        score_rank,
        score_rank_2,
        enrolled_at_rank,
        enrolled_at_rank_2,
        rows_in_course_id,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM metrics
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    score_change,
    prev_enrolled_at,
    second_enrolled_at,
    score_rank,
    score_rank_2,
    enrolled_at_rank,
    enrolled_at_rank_2,
    grade_status_slug,
    ROUND(score_rank / NULLIF(enrolled_at_rank_2, 0), 3) AS score_rank_per_enrolled_at_rank_2,
    prev_enrolled_at + INTERVAL 15 MINUTES AS prev_enrolled_at_plus_15m
FROM scored
