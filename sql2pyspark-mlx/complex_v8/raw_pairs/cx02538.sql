WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        LOWER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-24 13:00:00'
      AND grade_status = 'INCOMPLETE'
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM cleaned_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        grade_status_slug,
        term_prefix,
        term_clean,
        NTILE(4) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id) AS enrolled_at_quartile,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id, term), 2) AS score_vs_student_id_term_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status ORDER BY score, id), 3) AS score_cume_dist
    FROM filtered
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY grade_status, course_id) AS rows_in_grade_status_course_id,
        MIN(enrolled_at_quartile) OVER (PARTITION BY grade_status, course_id) AS grade_status_course_id_min_enrolled_at_quartile,
        ROUND(score_vs_student_id_term_avg - AVG(score_vs_student_id_term_avg) OVER (PARTITION BY grade_status, course_id), 2) AS score_vs_student_id_term_avg_vs_grade_status_cou,
        RANK() OVER w_all_by_score_vs_student_id_t AS score_vs_student_id_term_avg_rank,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY grade_status, course_id)) AS distinct_student_id_in_grade_status_course_id
    FROM sequenced
    WINDOW w_all_by_score_vs_student_id_t AS (ORDER BY score_vs_student_id_term_avg DESC)
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
    grade_status_slug,
    term_prefix,
    term_clean,
    first_known_score,
    score_vs_student_id_term_avg,
    score_cume_dist,
    rows_in_grade_status_course_id,
    score_vs_student_id_term_avg_vs_grade_status_cou,
    score_vs_student_id_term_avg_rank,
    distinct_student_id_in_grade_status_course_id,
    CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref,
    CASE WHEN score_cume_dist >= 0.8 THEN 'A' WHEN score_cume_dist >= 0.3 THEN 'B' ELSE 'C' END AS score_cume_dist_tier
FROM sequenced_2
WHERE course_id IS NOT NULL
ORDER BY student_id, term_clean DESC
