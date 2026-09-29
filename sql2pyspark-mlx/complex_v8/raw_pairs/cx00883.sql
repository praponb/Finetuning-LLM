WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2024T2', '2025T2') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        MAX(score) OVER (PARTITION BY grade_status ORDER BY score NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_max_6_prior,
        ROW_NUMBER() OVER w_grade_status_by_score AS rn,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY grade_status ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        LEAD(enrolled_at_date, 2) OVER (PARTITION BY grade_status ORDER BY score NULLS LAST, id) AS next2_enrolled_at_date,
        SIZE(COLLECT_SET(student_id) OVER w_grade_status) AS distinct_student_id_in_grade_status,
        score - LAG(score) OVER w_term_grade_status_by_score AS score_change,
        COUNT(*) OVER w_course_id_grade_status AS rows_in_course_id_grade_status,
        ROUND(AVG(score) OVER (PARTITION BY grade_status ORDER BY score NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        DENSE_RANK() OVER w_grade_status_by_score_ties AS score_dense_rank,
        ROUND(score - AVG(score) OVER w_course_id_grade_status, 2) AS score_vs_course_id_grade_status_avg
    FROM cleaned
    WINDOW w_grade_status_by_score AS (PARTITION BY grade_status ORDER BY score NULLS LAST, id), w_grade_status AS (PARTITION BY grade_status), w_term_grade_status_by_score AS (PARTITION BY term, grade_status ORDER BY score, id), w_course_id_grade_status AS (PARTITION BY course_id, grade_status), w_grade_status_by_score_ties AS (PARTITION BY grade_status ORDER BY score NULLS LAST)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        INITCAP(TRIM(term)) AS term_clean,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM ranked
    WHERE score_max_6_prior <= 240.0
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(rows_in_grade_status - AVG(rows_in_grade_status) OVER (PARTITION BY distinct_student_id_in_grade_status), 2) AS rows_in_grade_status_vs_distinct_student_id_in_g,
        ROUND(AVG(score_avg_7) OVER (ORDER BY rows_in_course_id_grade_status, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7_avg_7,
        NTILE(10) OVER w_all_by_score_vs_course_id_gr AS score_vs_course_id_grade_status_avg_decile,
        DENSE_RANK() OVER (ORDER BY score_vs_course_id_grade_status_avg) AS score_vs_course_id_grade_status_avg_dense_rank,
        ROUND(score_change / NULLIF(SUM(score_change) OVER (PARTITION BY distinct_student_id_in_grade_status), 0), 4) AS score_change_share_of_distinct_student_id_in_gra
    FROM derived
    WINDOW w_all_by_score_vs_course_id_gr AS (ORDER BY score_vs_course_id_grade_status_avg, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    rn,
    rows_in_grade_status,
    second_enrolled_at,
    next2_enrolled_at_date,
    distinct_student_id_in_grade_status,
    score_change,
    score_avg_7,
    score_dense_rank,
    score_vs_course_id_grade_status_avg,
    score_avg_7_avg_7,
    score_vs_course_id_grade_status_avg_decile,
    score_vs_course_id_grade_status_avg_dense_rank,
    score_change_share_of_distinct_student_id_in_gra,
    COALESCE(term, 'UNKNOWN') AS term_or_unknown
FROM with_windows
ORDER BY course_id, id
LIMIT 5
