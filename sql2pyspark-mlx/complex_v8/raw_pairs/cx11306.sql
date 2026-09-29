WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE term NOT IN ('2025T1', '2024T1')
),
with_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        st.is_international AS student_is_international,
        st.major AS student_major,
        c.dept AS course_dept,
        c.credits AS course_credits
    FROM recent_enrollments s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
    WHERE st.is_international = TRUE
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_student_major_course_id_by_s, 3) AS score_cume_dist,
        LEAD(score, 3) OVER (ORDER BY score ASC NULLS LAST, id) AS next3_score,
        NTH_VALUE(score, 2) OVER (ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id, student_major), 0), 4) AS score_share_of_student_id_student_major,
        SIZE(COLLECT_SET(term) OVER w_student_id_student_major) AS distinct_term_in_student_id_student_major,
        NTILE(3) OVER (PARTITION BY student_major, course_id ORDER BY score DESC, id) AS score_tercile,
        NTILE(5) OVER (PARTITION BY student_major, course_id ORDER BY score DESC, id) AS score_quintile,
        MAX(course_credits) OVER (PARTITION BY student_id, student_major) AS student_id_student_major_max_course_credits,
        DENSE_RANK() OVER w_student_major_course_id_by_s_2 AS score_dense_rank,
        SIZE(COLLECT_SET(grade_status) OVER w_student_id_student_major) AS distinct_grade_status_in_student_id_student_majo,
        NTH_VALUE(course_credits, 2) OVER (ORDER BY score ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_course_credits,
        SUM(score) OVER (ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score
    FROM with_details_student
    WINDOW w_student_major_course_id_by_s AS (PARTITION BY student_major, course_id ORDER BY score DESC, id), w_student_id_student_major AS (PARTITION BY student_id, student_major), w_student_major_course_id_by_s_2 AS (PARTITION BY student_major, course_id ORDER BY score DESC)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(score_dense_rank) IGNORE NULLS OVER (PARTITION BY student_id, course_credits ORDER BY second_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_dense_rank,
        ROUND(score_dense_rank - AVG(score_dense_rank) OVER (PARTITION BY student_id, score_tercile), 2) AS score_dense_rank_vs_student_id_score_tercile_avg,
        ROUND(AVG(running_score) OVER (ORDER BY course_credits DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS running_score_avg_3_prior,
        ROUND(score_tercile - AVG(score_tercile) OVER (PARTITION BY student_id, score_tercile), 2) AS score_tercile_vs_student_id_score_tercile_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id, course_credits ORDER BY second_score), 3) AS second_score_cume_dist,
        SIZE(COLLECT_SET(course_dept) OVER (PARTITION BY student_id, score_tercile)) AS distinct_course_dept_in_student_id_score_tercile,
        ROUND(AVG(score_dense_rank) OVER (PARTITION BY student_id, course_credits ORDER BY second_score, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_dense_rank_avg_3
    FROM windowed
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    student_is_international,
    student_major,
    course_credits,
    score_cume_dist,
    next3_score,
    score_share_of_student_id_student_major,
    distinct_term_in_student_id_student_major,
    score_tercile,
    student_id_student_major_max_course_credits,
    distinct_grade_status_in_student_id_student_majo,
    second_course_credits,
    running_score,
    last_known_score_dense_rank,
    running_score_avg_3_prior,
    distinct_course_dept_in_student_id_score_tercile,
    score_dense_rank_avg_3,
    COALESCE(score, 0.0) AS score_filled,
    ABS(score_dense_rank - 6) AS score_dense_rank_distance
FROM with_windows
ORDER BY score_cume_dist DESC, score_filled DESC
