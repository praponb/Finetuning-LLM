WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        CASE WHEN score >= 66.0 THEN 'A' WHEN score >= 38.5 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE course_id <= 2
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        FIRST_VALUE(score_filled) OVER (ORDER BY score_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score_filled,
        MAX(score_filled) OVER (PARTITION BY student_id) AS student_id_max_score_filled,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, score_tier ORDER BY score_filled NULLS LAST, id), 3) AS score_filled_percent_rank,
        ROUND(score_filled - AVG(score_filled) OVER (PARTITION BY student_id), 2) AS score_filled_vs_student_id_avg,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY student_id)) AS distinct_term_in_student_id
    FROM base_enrollments
)
-- Subtotals with ROLLUP
SELECT
    grade_status,
    course_id,
    COUNT(DISTINCT course_id) AS distinct_course_ids,
    SUM(first_score_filled) FILTER (WHERE grade_status = 'PASS') AS first_score_filled_grade_status_pass
FROM metrics
GROUP BY ROLLUP(grade_status, course_id)
ORDER BY grade_status NULLS LAST, course_id NULLS LAST
