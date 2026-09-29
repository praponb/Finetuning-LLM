WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE term IN ('2025T2', '2025T1', '2024T2')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(score) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        RANK() OVER (ORDER BY score DESC) AS score_rank,
        LAG(score) OVER (ORDER BY score DESC, id) AS prev_score,
        ROUND(AVG(score) OVER w_grade_status_course_id, 2) AS grade_status_course_id_avg_score,
        score - LAG(score) OVER w_all_by_score AS score_change,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status, course_id), 0), 4) AS score_share_of_grade_status_course_id
    FROM filtered
    WINDOW w_grade_status_course_id AS (PARTITION BY grade_status, course_id), w_all_by_score AS (ORDER BY score DESC, id)
),
with_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.prev_score,
        s.grade_status_course_id_avg_score,
        s.score_share_of_grade_status_course_id,
        st.gpa AS student_gpa,
        st.major AS student_major
    FROM with_windows s
    LEFT JOIN students st ON st.id = s.student_id AND st.enrolled_on BETWEEN DATE '2024-02-28' AND DATE '2025-07-18'
),
enriched_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.prev_score,
        s.score_share_of_grade_status_course_id,
        c.credits AS course_credits
    FROM with_details s
    JOIN courses c ON c.id = s.course_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        score_share_of_grade_status_course_id,
        course_credits,
        ROUND(PERCENT_RANK() OVER w_course_id_grade_status_by_pr, 3) AS prev_score_percent_rank,
        ROUND(CUME_DIST() OVER w_course_id_grade_status_by_pr, 3) AS prev_score_cume_dist,
        LAG(score, 3) OVER w_all_by_prev_score AS prev3_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY prev_score ASC), 3) AS prev_score_percent_rank_2,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_credits)) AS distinct_grade_status_in_course_credits,
        FIRST_VALUE(prev_score) OVER (PARTITION BY course_id, grade_status ORDER BY prev_score DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prev_score
    FROM enriched_course
    WINDOW w_course_id_grade_status_by_pr AS (PARTITION BY course_id, grade_status ORDER BY prev_score DESC NULLS FIRST), w_all_by_prev_score AS (ORDER BY prev_score ASC, id)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    score_share_of_grade_status_course_id,
    course_credits,
    prev_score_percent_rank,
    prev_score_cume_dist,
    prev3_score,
    prev_score_percent_rank_2,
    distinct_grade_status_in_course_credits,
    first_prev_score,
    LENGTH(TRIM(grade_status)) AS grade_status_len,
    LEAST(prev3_score, course_credits) AS least_prev3_score_course_credits
FROM with_trends
ORDER BY course_id NULLS LAST, term
