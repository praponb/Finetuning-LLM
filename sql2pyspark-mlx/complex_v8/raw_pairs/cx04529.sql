WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2024T2', '2025T2') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group,
        INITCAP(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE grade_status = 'INCOMPLETE'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_group,
        term_clean,
        RANK() OVER w_grade_status_by_score_ties AS score_rank,
        ROUND(AVG(score) OVER w_term_group_term, 2) AS term_group_term_avg_score,
        score - LAG(score) OVER (ORDER BY enrolled_at NULLS LAST, id) AS score_change,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_cume_dist,
        ROUND(AVG(score) OVER (ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        LEAD(enrolled_at) OVER w_student_id_by_enrolled_at AS next_enrolled_at,
        ROUND(score / NULLIF(SUM(score) OVER w_course_id_term_group, 0), 4) AS score_share_of_course_id_term_group,
        MIN(score) OVER (PARTITION BY term_group, term) AS term_group_term_min_score,
        SUM(score) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_sum_2,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at ASC), 3) AS enrolled_at_cume_dist_2,
        MIN(score) OVER (PARTITION BY term) AS term_min_score,
        ROUND(score - AVG(score) OVER w_term_group_term, 2) AS score_vs_term_group_term_avg,
        RANK() OVER (ORDER BY enrolled_at ASC) AS enrolled_at_rank,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        RANK() OVER w_student_id_by_enrolled_at_ti AS enrolled_at_rank_2
    FROM prepared_enrollments
    WINDOW w_grade_status_by_score_ties AS (PARTITION BY grade_status ORDER BY score ASC), w_term_group_term AS (PARTITION BY term_group, term), w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at DESC, id), w_course_id_term_group AS (PARTITION BY course_id, term_group), w_student_id_by_enrolled_at_ti AS (PARTITION BY student_id ORDER BY enrolled_at DESC)
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    term_group,
    term_clean,
    term_group_term_avg_score,
    score_change,
    enrolled_at_cume_dist,
    enrolled_at_cume_dist_2,
    term_min_score,
    score_vs_term_group_term_avg,
    rows_in_term,
    enrolled_at_rank_2,
    COALESCE(rows_in_term, 0) AS rows_in_term_filled,
    DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
FROM sequenced
ORDER BY days_since_enrolled_at NULLS LAST, id
LIMIT 10
