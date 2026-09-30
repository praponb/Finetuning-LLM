WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
      AND score BETWEEN 5.6 AND 57.8
),
joined AS (
    -- Attach student attributes
    SELECT s.*, st.student_name, c.dept AS course_dept
    FROM base_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        score_filled,
        student_name,
        course_dept,
        COUNT(*) OVER (PARTITION BY grade_status, term) AS rows_in_grade_status_term,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY grade_status, term)) AS distinct_course_id_in_grade_status_term,
        RANK() OVER (PARTITION BY student_id ORDER BY enrolled_at) AS enrolled_at_rank,
        ROUND(score_filled / NULLIF(SUM(score_filled) OVER (PARTITION BY grade_status, term), 0), 4) AS score_filled_share_of_grade_status_term,
        SUM(score_filled) OVER w_grade_status_term AS grade_status_term_sum_score_filled,
        ROUND(AVG(score_filled) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_filled_avg_6_prior,
        MAX(score_filled) OVER (PARTITION BY grade_status, term) AS grade_status_term_max_score_filled,
        NTILE(4) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS enrolled_at_quartile,
        LAST_VALUE(score_filled) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_filled
    FROM joined
    WINDOW w_grade_status_term AS (PARTITION BY grade_status, term)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        score_filled,
        student_name,
        course_dept,
        rows_in_grade_status_term,
        distinct_course_id_in_grade_status_term,
        score_filled_share_of_grade_status_term,
        grade_status_term_sum_score_filled,
        score_filled_avg_6_prior,
        grade_status_term_max_score_filled,
        enrolled_at_quartile,
        last_known_score_filled
    FROM metrics
    WHERE enrolled_at_rank <= 2
      AND enrolled_at_quartile <= 6
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(last_known_score_filled) OVER (PARTITION BY distinct_course_id_in_grade_status_term, DATE_TRUNC('MONTH', enrolled_at)) AS distinct_course_id_in_grade_status_term_enrolled,
        DENSE_RANK() OVER w_distinct_course_id_in_grade_2 AS enrolled_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_course_id_in_grade_status_term ORDER BY enrolled_at DESC), 3) AS enrolled_at_cume_dist
    FROM leaders
    WINDOW w_distinct_course_id_in_grade_2 AS (PARTITION BY distinct_course_id_in_grade_status_term ORDER BY enrolled_at DESC)
),
summary AS (
    -- Group-level totals
    SELECT
        distinct_course_id_in_grade_status_term,
        grade_status,
        course_dept,
        COUNT(*) AS cnt,
        ROUND(SUM(grade_status_term_max_score_filled) / NULLIF(SUM(distinct_course_id_in_grade_status_term_enrolled), 0), 3) AS grade_status_term_max_score_filled_to_distinct_c
    FROM with_trends
    WHERE enrolled_at < TIMESTAMP '2025-02-18 10:00:00'
    GROUP BY distinct_course_id_in_grade_status_term, grade_status, course_dept
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(cnt / NULLIF(SUM(cnt) OVER (PARTITION BY cnt, distinct_course_id_in_grade_status_term), 0), 4) AS cnt_share_of_cnt_distinct_course_id_in_grade_sta,
        ROUND(AVG(cnt) OVER (PARTITION BY distinct_course_id_in_grade_status_term, course_dept ORDER BY cnt, distinct_course_id_in_grade_status_term, grade_status, course_dept ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS cnt_avg_2,
        ROUND(PERCENT_RANK() OVER w_distinct_course_id_in_grade_4, 3) AS cnt_percent_rank
    FROM summary
    WINDOW w_distinct_course_id_in_grade_4 AS (PARTITION BY distinct_course_id_in_grade_status_term, course_dept ORDER BY cnt)
)
-- Final projection
SELECT
    distinct_course_id_in_grade_status_term,
    grade_status,
    course_dept,
    cnt,
    grade_status_term_max_score_filled_to_distinct_c,
    cnt_share_of_cnt_distinct_course_id_in_grade_sta,
    cnt_avg_2,
    cnt_percent_rank,
    CONCAT(grade_status, '#', CAST(cnt AS STRING)) AS grade_status_ref,
    LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
FROM ranked
ORDER BY grade_status
