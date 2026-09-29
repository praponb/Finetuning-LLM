WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        ABS(score - 57.6) AS score_distance,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        LOWER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE score < 78.8
      AND enrolled_at < TIMESTAMP '2025-03-08 09:00:00'
),
enriched AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score_distance,
        s.term_clean,
        c.dept AS course_dept,
        c.credits AS course_credits,
        c.level AS course_level,
        st.is_international AS student_is_international
    FROM base s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY course_dept, course_id), 2) AS course_credits_vs_course_dept_course_id_avg,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER w_course_dept_course_id, 0), 4) AS score_distance_share_of_course_dept_course_id,
        MAX(course_credits) OVER (PARTITION BY course_dept, course_id) AS course_dept_course_id_max_course_credits,
        ROUND(AVG(score_distance) OVER (PARTITION BY course_dept, course_id), 2) AS course_dept_course_id_avg_score_distance
    FROM enriched
    WINDOW w_course_dept_course_id AS (PARTITION BY course_dept, course_id)
)
SELECT id, field, metric_value
FROM (
    SELECT id, course_dept_course_id_max_course_credits, course_credits
    FROM windowed
) src
UNPIVOT (
    metric_value FOR field IN (course_dept_course_id_max_course_credits, course_credits)
) unp
