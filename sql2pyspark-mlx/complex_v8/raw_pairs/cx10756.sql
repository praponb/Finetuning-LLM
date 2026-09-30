WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label,
        ROUND(score * 1.1, 2) AS score_adj
    FROM enrollments
    WHERE term <> '2024T2'
),
with_parent AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.grade_status_term_label,
        s.score_adj,
        st.is_international AS student_is_international,
        c.course_code
    FROM base s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        grade_status_term_label,
        score_adj,
        student_is_international,
        course_code,
        MIN(score_adj) OVER (PARTITION BY YEAR(enrolled_at)) AS enrolled_at_year_min_score_adj,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        SUM(score_adj) OVER (PARTITION BY course_id ORDER BY score_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_adj,
        LEAD(enrolled_at, 3) OVER w_all_by_score_adj AS next3_enrolled_at,
        ROUND(score_adj - AVG(score_adj) OVER w_student_is_international, 2) AS score_adj_vs_student_is_international_avg,
        LEAD(enrolled_at, 2) OVER (PARTITION BY course_id ORDER BY score_adj, id) AS next2_enrolled_at,
        NTH_VALUE(score_adj, 2) OVER (PARTITION BY course_id ORDER BY score_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_adj,
        MAX(score_adj) OVER (PARTITION BY course_id ORDER BY score_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_adj_so_far,
        ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY score_adj, id) AS rn,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY student_is_international, course_id)) AS distinct_term_in_student_is_international_course,
        FIRST_VALUE(score_adj) IGNORE NULLS OVER (PARTITION BY course_id ORDER BY score_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score_adj
    FROM with_parent
    WINDOW w_all_by_score_adj AS (ORDER BY score_adj ASC, id), w_student_is_international AS (PARTITION BY student_is_international)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        grade_status_term_label,
        score_adj,
        student_is_international,
        course_code,
        running_score_adj,
        score_adj_vs_student_is_international_avg,
        next2_enrolled_at,
        second_score_adj,
        max_score_adj_so_far,
        rn,
        MAX(max_score_adj_so_far) OVER (PARTITION BY course_id ORDER BY score_adj_vs_student_is_international_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_max_score_adj_so_far_so_far,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY score_adj_vs_student_is_international_avg DESC, id) AS score_adj_vs_student_is_international_avg_dense_,
        NTH_VALUE(last_enrolled_at, 2) OVER (PARTITION BY course_id ORDER BY score_adj_vs_student_is_international_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_enrolled_at,
        LAG(rn) OVER (PARTITION BY term ORDER BY max_score_adj_so_far ASC, id) AS prev_rn
    FROM with_trends
)
-- Final projection
SELECT DISTINCT id
FROM ranked
ORDER BY id
