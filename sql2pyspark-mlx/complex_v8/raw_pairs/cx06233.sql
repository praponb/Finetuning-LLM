WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        CONCAT_WS('-', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND score <= 59.8
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_slug,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        score - LAG(score) OVER w_all_by_enrolled_at AS score_change,
        LAG(enrolled_at, 3) OVER w_all_by_enrolled_at AS prev3_enrolled_at,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id, term), 0), 4) AS score_share_of_course_id_term,
        ROUND(score - AVG(score) OVER w_course_id, 2) AS score_vs_course_id_avg,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id, term), 2) AS score_vs_course_id_term_avg,
        SUM(score) OVER w_course_id AS course_id_sum_score,
        LEAD(score, 3, 0) OVER (ORDER BY enrolled_at DESC, id) AS next3_score,
        score - LAG(score) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id) AS score_change_2,
        MAX(score) OVER (PARTITION BY course_id) AS course_id_max_score,
        RANK() OVER (ORDER BY enrolled_at DESC) AS enrolled_at_rank,
        LEAD(score, 3, 0) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id) AS next3_score_2
    FROM filtered_enrollments
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at, id), w_course_id AS (PARTITION BY course_id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE enrolled_at_rank <= 3
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        grade_status,
        course_id,
        COUNT(*) FILTER (WHERE grade_status NOT IN ('FAIL', 'PASS')) AS n_grade_status_not_in_fail_pas,
        SUM(CASE WHEN grade_status IS NOT NULL THEN 1 ELSE 0 END) AS n_grade_status_is_not_null
    FROM top_per_group
    WHERE score_share_of_course_id_term >= 0.2
    GROUP BY grade_status, course_id
)
SELECT
    grade_status,
    n_grade_status_not_in_fail_pas,
    SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix,
    CASE WHEN grade_status IN ('WITHDRAWN', 'FAIL') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group
FROM stats
WHERE course_id IS NOT NULL
ORDER BY grade_status_prefix, grade_status DESC
