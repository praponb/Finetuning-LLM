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
        CASE WHEN grade_status IN ('INCOMPLETE', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE course_id IS NOT NULL
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
        s.grade_status_group,
        s.grade_status_term_label,
        c.course_code,
        c.level AS course_level,
        st.student_name,
        st.enrolled_on AS student_enrolled_on
    FROM filtered_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        grade_status_group,
        grade_status_term_label,
        course_code,
        course_level,
        student_name,
        ROUND(CUME_DIST() OVER (PARTITION BY term ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY term ORDER BY enrolled_at) AS enrolled_at_dense_rank,
        RANK() OVER (ORDER BY score, id) AS score_rank,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        LAST_VALUE(student_enrolled_on) OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_student_enrolled_on,
        ROUND(AVG(score) OVER (PARTITION BY student_id), 2) AS student_id_avg_score,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id), 2) AS score_vs_student_id_avg,
        ROUND(AVG(score) OVER (ORDER BY student_enrolled_on, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        score - LAG(score) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS score_change,
        RANK() OVER (ORDER BY student_enrolled_on) AS student_enrolled_on_rank,
        LAG(enrolled_at) OVER (ORDER BY score, id) AS prev_enrolled_at,
        SUM(score) OVER (PARTITION BY student_id) AS student_id_sum_score,
        SIZE(COLLECT_SET(course_level) OVER (PARTITION BY student_id)) AS distinct_course_level_in_student_id
    FROM with_details
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        grade_status_group,
        grade_status_term_label,
        course_code,
        course_level,
        student_name,
        enrolled_at_cume_dist,
        enrolled_at_dense_rank,
        rows_in_student_id,
        last_student_enrolled_on,
        student_id_avg_score,
        score_vs_student_id_avg,
        score_avg_3_prior,
        score_change,
        student_enrolled_on_rank,
        prev_enrolled_at,
        student_id_sum_score,
        distinct_course_level_in_student_id
    FROM ranked
    WHERE score_rank = 1
      AND course_id <= 10
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    grade_status,
    enrolled_at,
    grade_status_term_label,
    course_code,
    course_level,
    student_name,
    enrolled_at_cume_dist,
    enrolled_at_dense_rank,
    rows_in_student_id,
    last_student_enrolled_on,
    student_id_avg_score,
    score_vs_student_id_avg,
    score_avg_3_prior,
    student_enrolled_on_rank,
    prev_enrolled_at,
    student_id_sum_score,
    distinct_course_level_in_student_id,
    score_change + rows_in_student_id AS score_change_plus_rows_in_student_id
FROM top_rows
ORDER BY term, course_code DESC
