WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
combined_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        st.enrolled_on AS student_enrolled_on
    FROM valid_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term,
        grade_status,
        student_enrolled_on,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status, course_id), 0), 4) AS score_share_of_grade_status_course_id,
        LAG(score) OVER w_grade_status_by_student_enro AS prev_score,
        SIZE(COLLECT_SET(course_id) OVER w_term) AS distinct_course_id_in_term,
        NTH_VALUE(student_enrolled_on, 2) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_enrolled_on,
        COUNT(*) OVER (PARTITION BY grade_status, course_id) AS rows_in_grade_status_course_id,
        LEAD(score, 1, 0) OVER (PARTITION BY grade_status ORDER BY student_enrolled_on, id) AS next_score,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id, term), 2) AS score_vs_course_id_term_avg,
        DENSE_RANK() OVER w_grade_status_by_student_enro AS student_enrolled_on_dense_rank,
        NTILE(10) OVER (PARTITION BY student_id, term ORDER BY student_enrolled_on, id) AS student_enrolled_on_decile,
        LAST_VALUE(score) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        ROUND(AVG(score) OVER (ORDER BY score, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior,
        ROW_NUMBER() OVER w_all_by_score AS seq_no,
        ROUND(PERCENT_RANK() OVER (ORDER BY score), 3) AS score_percent_rank,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY term)) AS distinct_grade_status_in_term,
        FIRST_VALUE(student_enrolled_on) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY student_enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_student_enrolled_on
    FROM combined_details
    WINDOW w_grade_status_by_student_enro AS (PARTITION BY grade_status ORDER BY student_enrolled_on, id), w_term AS (PARTITION BY term), w_all_by_score AS (ORDER BY score, id)
)
-- Final projection
SELECT
    id,
    term,
    student_enrolled_on,
    prev_score,
    distinct_course_id_in_term,
    second_student_enrolled_on,
    rows_in_grade_status_course_id,
    next_score,
    score_vs_course_id_term_avg,
    student_enrolled_on_dense_rank,
    last_score,
    score_avg_6_prior,
    seq_no,
    score_percent_rank,
    distinct_grade_status_in_term,
    first_known_student_enrolled_on
FROM with_windows
WHERE term = '2025T2'
ORDER BY seq_no
