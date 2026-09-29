WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 2)
      AND course_id IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.term_slug,
        c.level AS course_level
    FROM scoped s
    JOIN courses c ON c.id = s.course_id
    WHERE c.level = 'advanced'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        course_level,
        RANK() OVER (PARTITION BY student_id ORDER BY score, id) AS score_rank,
        ROW_NUMBER() OVER (ORDER BY enrolled_at DESC, id) AS enrolled_at_seq,
        SUM(score) OVER (PARTITION BY grade_status, course_id) AS grade_status_course_id_sum_score,
        NTILE(4) OVER w_all_by_score AS score_quartile,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY student_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        FIRST_VALUE(score) IGNORE NULLS OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        MIN(score) OVER (PARTITION BY grade_status, course_id) AS grade_status_course_id_min_score,
        LEAD(score) OVER w_grade_status_student_id_by_s AS next_score,
        COUNT(*) OVER (PARTITION BY student_id, grade_status ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status, course_level), 2) AS score_vs_grade_status_course_level_avg,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status, course_level), 0), 4) AS score_share_of_grade_status_course_level,
        DENSE_RANK() OVER (PARTITION BY grade_status, student_id ORDER BY score) AS score_dense_rank,
        LAG(score, 2) OVER (ORDER BY score, id) AS prev2_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_level), 0), 4) AS score_share_of_course_level
    FROM with_parent
    WINDOW w_all_by_score AS (ORDER BY score, id), w_grade_status_student_id_by_s AS (PARTITION BY grade_status, student_id ORDER BY score, id)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(course_level, '#', CAST(enrolled_at_seq AS STRING)) AS course_level_ref
    FROM with_windows
    WHERE student_id <= 4
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        score,
        grade_status,
        enrolled_at,
        enrolled_at_seq,
        score_quartile,
        first_known_score,
        grade_status_course_id_min_score,
        next_score,
        running_count,
        score_vs_grade_status_course_level_avg,
        score_share_of_grade_status_course_level,
        prev2_score,
        score_share_of_course_level,
        course_level_ref,
        ROUND(first_known_score - AVG(first_known_score) OVER w_score_dense_rank, 2) AS first_known_score_vs_score_dense_rank_avg,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY score_dense_rank)) AS distinct_grade_status_in_score_dense_rank,
        LAST_VALUE(score_dense_rank) OVER (PARTITION BY enrolled_at_seq ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_dense_rank,
        ROW_NUMBER() OVER w_student_id_course_level_by_e AS rn,
        SUM(enrolled_at_seq) OVER (PARTITION BY student_id, course_level ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_enrolled_at_seq,
        NTH_VALUE(score, 2) OVER (PARTITION BY enrolled_at_seq ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        DENSE_RANK() OVER (PARTITION BY enrolled_at_seq ORDER BY score DESC) AS score_dense_rank_2
    FROM scored
    WINDOW w_score_dense_rank AS (PARTITION BY score_dense_rank), w_student_id_course_level_by_e AS (PARTITION BY student_id, course_level ORDER BY enrolled_at, id)
)
SELECT
    id,
    student_id,
    score,
    grade_status,
    enrolled_at,
    score_quartile,
    first_known_score,
    next_score,
    running_count,
    score_vs_grade_status_course_level_avg,
    score_share_of_grade_status_course_level,
    prev2_score,
    score_share_of_course_level,
    course_level_ref,
    distinct_grade_status_in_score_dense_rank,
    running_enrolled_at_seq,
    second_score,
    COALESCE(score_dense_rank_2, 0) AS score_dense_rank_2_filled,
    ROUND(score_dense_rank_2 / NULLIF(prev2_score, 0), 3) AS score_dense_rank_2_per_prev2_score
FROM windowed
ORDER BY score_vs_grade_status_course_level_avg DESC NULLS FIRST, score_dense_rank_2_filled
