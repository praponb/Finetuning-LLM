WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        HOUR(enrolled_at) AS enrolled_at_hour
    FROM enrollments
    WHERE student_id <= 4
      AND grade_status <> 'PASS'
),
combined_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at_hour,
        c.level AS course_level
    FROM recent s
    JOIN courses c ON c.id = s.course_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        ROUND(score - AVG(score) OVER (PARTITION BY course_level), 2) AS score_vs_course_level_avg,
        LAST_VALUE(score) OVER (PARTITION BY course_level, course_id ORDER BY enrolled_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        SIZE(COLLECT_SET(course_level) OVER w_course_id_enrolled_at_hour) AS distinct_course_level_in_course_id_enrolled_at_h,
        DENSE_RANK() OVER (PARTITION BY course_id, student_id ORDER BY score ASC) AS score_dense_rank,
        COUNT(*) OVER w_course_id_enrolled_at_hour AS rows_in_course_id_enrolled_at_hour,
        MAX(score) OVER (PARTITION BY course_level) AS course_level_max_score,
        LEAD(score, 2, 0) OVER (PARTITION BY course_level, course_id ORDER BY enrolled_at_hour DESC, id) AS next2_score,
        ROUND(enrolled_at_hour / NULLIF(SUM(enrolled_at_hour) OVER w_course_level, 0), 4) AS enrolled_at_hour_share_of_course_level
    FROM combined_details
    WINDOW w_course_id_enrolled_at_hour AS (PARTITION BY course_id, enrolled_at_hour), w_course_level AS (PARTITION BY course_level)
)
-- Final projection
SELECT
    id,
    course_id,
    score_vs_course_level_avg,
    score_dense_rank,
    next2_score,
    LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
    ROUND(next2_score * 0.25, 2) AS next2_score_adj
FROM ranked
ORDER BY course_id NULLS LAST, score_vs_course_level_avg DESC NULLS FIRST
