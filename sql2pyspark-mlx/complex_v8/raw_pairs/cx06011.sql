WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-24 13:00:00'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term,
        score,
        enrolled_at,
        MAX(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_max_2,
        ROUND(AVG(score) OVER (PARTITION BY student_id), 2) AS student_id_avg_score,
        LAG(enrolled_at, 3) OVER w_course_id_student_id_by_scor AS prev3_enrolled_at,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        ROUND(AVG(score) OVER (PARTITION BY TO_DATE(enrolled_at)), 2) AS enrolled_at_to_date_avg_score,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id), 2) AS score_vs_student_id_avg,
        SIZE(COLLECT_SET(student_id) OVER w_enrolled_at_to_date) AS distinct_student_id_in_enrolled_at_to_date,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id, 0), 4) AS score_share_of_student_id
    FROM valid
    WINDOW w_course_id_student_id_by_scor AS (PARTITION BY course_id, student_id ORDER BY score DESC, id), w_enrolled_at_to_date AS (PARTITION BY TO_DATE(enrolled_at)), w_student_id AS (PARTITION BY student_id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        course_id,
        term,
        score_max_2,
        student_id_avg_score,
        score_vs_student_id_avg,
        distinct_student_id_in_enrolled_at_to_date,
        score_share_of_student_id,
        SUM(score_vs_student_id_avg) OVER (PARTITION BY YEAR(enrolled_at) ORDER BY score, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_vs_student_id_avg_sum_3_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(enrolled_at) ORDER BY score, id), 3) AS score_percent_rank,
        ROUND(score_max_2 / NULLIF(SUM(score_max_2) OVER (PARTITION BY course_id), 0), 4) AS score_max_2_share_of_course_id,
        SIZE(COLLECT_SET(term) OVER w_course_id) AS distinct_term_in_course_id,
        RANK() OVER w_course_id_term_by_enrolled_a AS enrolled_at_rank,
        ROW_NUMBER() OVER (PARTITION BY YEAR(enrolled_at) ORDER BY score, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id
    FROM windowed
    WINDOW w_course_id AS (PARTITION BY course_id), w_course_id_term_by_enrolled_a AS (PARTITION BY course_id, term ORDER BY enrolled_at ASC, id)
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score_max_2,
    student_id_avg_score,
    distinct_student_id_in_enrolled_at_to_date,
    score_share_of_student_id,
    score_vs_student_id_avg_sum_3_prior,
    score_max_2_share_of_course_id,
    distinct_term_in_course_id,
    enrolled_at_rank,
    rows_in_course_id,
    LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
    GREATEST(score_vs_student_id_avg, rows_in_course_id) AS greatest_score_vs_student_id_avg_rows_in_course_
FROM with_windows
