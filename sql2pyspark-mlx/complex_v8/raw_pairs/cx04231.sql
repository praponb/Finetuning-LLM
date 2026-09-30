WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE term = '2025T1'
      AND course_id <= 2
),
enriched AS (
    -- Attach course attributes
    SELECT
        s.*,
        c.credits AS course_credits,
        c.course_code,
        c.dept AS course_dept
    FROM recent s
    LEFT JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status_len,
        grade_status_slug,
        course_credits,
        course_code,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY grade_status_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        RANK() OVER (PARTITION BY student_id ORDER BY grade_status_len DESC, id) AS grade_status_len_rank,
        LAST_VALUE(grade_status_len) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_grade_status_len,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_2,
        COUNT(*) OVER w_grade_status AS rows_in_grade_status,
        RANK() OVER (ORDER BY grade_status_len DESC NULLS FIRST, id) AS grade_status_len_rank_2,
        RANK() OVER (PARTITION BY student_id ORDER BY grade_status_len DESC) AS grade_status_len_rank_3,
        ROW_NUMBER() OVER w_grade_status_by_score AS rn,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_id)) AS distinct_term_in_course_id,
        ROUND(AVG(course_credits) OVER (ORDER BY grade_status_len DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS course_credits_avg_7,
        SUM(score) OVER (ORDER BY grade_status_len DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_sum_3_prior,
        NTILE(4) OVER w_grade_status_by_score AS score_quartile,
        LEAD(score, 2) OVER (ORDER BY grade_status_len DESC NULLS FIRST, id) AS next2_score
    FROM enriched
    WINDOW w_grade_status AS (PARTITION BY grade_status), w_grade_status_by_score AS (PARTITION BY grade_status ORDER BY score DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status_len,
        grade_status_slug,
        course_credits,
        course_code,
        last_known_score,
        last_grade_status_len,
        last_known_score_2,
        rows_in_grade_status,
        grade_status_len_rank_2,
        grade_status_len_rank_3,
        rn,
        distinct_term_in_course_id,
        score_quartile,
        next2_score,
        FIRST_VALUE(course_credits) IGNORE NULLS OVER (PARTITION BY grade_status_len_rank_2 ORDER BY last_known_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_course_credits,
        COUNT(*) OVER (PARTITION BY grade_status_len_rank, distinct_term_in_course_id) AS rows_in_grade_status_len_rank_distinct_term_in_c,
        ROUND(rows_in_grade_status - AVG(rows_in_grade_status) OVER w_grade_status_len_rank_distin, 2) AS rows_in_grade_status_vs_grade_status_len_rank_di
    FROM sequenced
    WINDOW w_grade_status_len_rank_distin AS (PARTITION BY grade_status_len_rank, distinct_term_in_course_id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status_slug,
    course_credits,
    course_code,
    last_known_score,
    last_grade_status_len,
    last_known_score_2,
    rows_in_grade_status,
    grade_status_len_rank_3,
    rn,
    distinct_term_in_course_id,
    next2_score,
    first_known_course_credits,
    rows_in_grade_status_len_rank_distinct_term_in_c,
    rows_in_grade_status_vs_grade_status_len_rank_di,
    ABS(grade_status_len - 9) AS grade_status_len_distance,
    ABS(grade_status_len - 7) AS grade_status_len_distance_2
FROM with_windows
ORDER BY first_known_course_credits, id
LIMIT 10
