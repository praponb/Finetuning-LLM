WITH prepared_enrollments AS (
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
    WHERE (course_id IS NOT NULL OR term IS NOT NULL)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        c.credits AS course_credits
    FROM prepared_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(score) OVER (PARTITION BY grade_status, student_id ORDER BY score DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_sum_2,
        SUM(course_credits) OVER (PARTITION BY student_id) AS student_id_sum_course_credits,
        MIN(course_credits) OVER w_course_credits AS course_credits_min_course_credits,
        ROUND(AVG(score) OVER (PARTITION BY student_id, course_credits ORDER BY score DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY student_id), 2) AS course_credits_vs_student_id_avg,
        ROUND(AVG(score) OVER (PARTITION BY course_credits), 2) AS course_credits_avg_score,
        course_credits - LAG(course_credits) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), course_credits ORDER BY enrolled_at NULLS LAST, id) AS course_credits_change,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_credits)) AS distinct_student_id_in_course_credits,
        ROUND(score - AVG(score) OVER (PARTITION BY course_credits), 2) AS score_vs_course_credits_avg,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id), 2) AS score_vs_student_id_avg,
        RANK() OVER (PARTITION BY grade_status, student_id ORDER BY score DESC, id) AS score_rank,
        NTILE(10) OVER w_enrolled_at_month_course_cre AS enrolled_at_decile,
        ROW_NUMBER() OVER (PARTITION BY student_id, course_credits ORDER BY score DESC, id) AS score_seq,
        NTILE(4) OVER w_course_id_student_id_by_scor AS score_quartile,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY course_credits), 2) AS course_credits_vs_course_credits_avg
    FROM with_details
    WINDOW w_course_credits AS (PARTITION BY course_credits), w_enrolled_at_month_course_cre AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), course_credits ORDER BY enrolled_at NULLS LAST, id), w_course_id_student_id_by_scor AS (PARTITION BY course_id, student_id ORDER BY score, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        score,
        grade_status,
        enrolled_at,
        score_sum_2,
        student_id_sum_course_credits,
        course_credits_vs_student_id_avg,
        course_credits_change,
        distinct_student_id_in_course_credits,
        score_vs_student_id_avg,
        score_rank,
        score_seq,
        score_quartile,
        course_credits_vs_course_credits_avg,
        COALESCE(score_quartile, 0) AS score_quartile_filled,
        INITCAP(TRIM(term)) AS term_clean,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM sequenced
    WHERE score_vs_course_credits_avg <= 50.0
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        grade_status,
        enrolled_at,
        score_sum_2,
        student_id_sum_course_credits,
        course_credits_vs_student_id_avg,
        course_credits_change,
        distinct_student_id_in_course_credits,
        score_vs_student_id_avg,
        course_credits_vs_course_credits_avg,
        score_quartile_filled,
        term_clean,
        ROUND(CUME_DIST() OVER (ORDER BY score_sum_2), 3) AS score_sum_2_cume_dist,
        COUNT(*) OVER (ORDER BY score_sum_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(score, 2) OVER (ORDER BY score_sum_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        ROUND(score_quartile_filled - AVG(score_quartile_filled) OVER (PARTITION BY grade_status, distinct_student_id_in_course_credits), 2) AS score_quartile_filled_vs_grade_status_distinct_s,
        NTILE(3) OVER (PARTITION BY score_seq, student_id ORDER BY score_rank NULLS LAST, id) AS score_rank_tercile,
        ROUND(AVG(course_credits_change) OVER (ORDER BY score_sum_2, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS course_credits_change_avg_3_prior
    FROM annotated
    WINDOW w_grade_status_distinct_studen AS (PARTITION BY grade_status, distinct_student_id_in_course_credits)
)
-- Final projection
SELECT
    id,
    grade_status,
    enrolled_at,
    student_id_sum_course_credits,
    course_credits_change,
    distinct_student_id_in_course_credits,
    course_credits_vs_course_credits_avg,
    score_quartile_filled,
    term_clean,
    score_sum_2_cume_dist,
    running_count,
    score_quartile_filled_vs_grade_status_distinct_s,
    score_rank_tercile,
    course_credits_change_avg_3_prior,
    DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
FROM sequenced_2
WHERE course_credits_change_avg_3_prior > -2.0
ORDER BY score_sum_2_cume_dist DESC, id
LIMIT 5
