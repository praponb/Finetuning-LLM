WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-25 13:00:00'
      AND student_id >= 6
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_student_id_by_score_ties, 3) AS score_percent_rank,
        DENSE_RANK() OVER (PARTITION BY student_id ORDER BY score DESC) AS score_dense_rank,
        NTH_VALUE(score, 2) OVER (PARTITION BY student_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_id)) AS distinct_student_id_in_course_id,
        MIN(score) OVER (PARTITION BY course_id) AS course_id_min_score,
        LEAD(enrolled_at, 2) OVER (PARTITION BY student_id ORDER BY score DESC, id) AS next2_enrolled_at
    FROM valid
    WINDOW w_student_id_by_score_ties AS (PARTITION BY student_id ORDER BY score DESC)
),
combined_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_percent_rank,
        s.score_dense_rank,
        s.second_score,
        s.next2_enrolled_at,
        c.course_code,
        c.level AS course_level,
        st.enrolled_on AS student_enrolled_on
    FROM with_trends s
    LEFT JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        score,
        enrolled_at,
        score_percent_rank,
        next2_enrolled_at,
        course_code,
        student_enrolled_on,
        LAG(score_percent_rank) OVER w_student_id_course_id_by_scor AS prev_score_percent_rank,
        NTILE(3) OVER (ORDER BY score_percent_rank DESC, id) AS score_percent_rank_tercile,
        SUM(score_dense_rank) OVER (PARTITION BY score_dense_rank, term) AS score_dense_rank_term_sum_score_dense_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY score_dense_rank, term), 0), 4) AS score_share_of_score_dense_rank_term
    FROM combined_details_course
    WINDOW w_student_id_course_id_by_scor AS (PARTITION BY student_id, course_id ORDER BY score ASC, id)
)
-- Final projection
SELECT
    id,
    enrolled_at,
    next2_enrolled_at,
    course_code,
    prev_score_percent_rank,
    score_percent_rank_tercile,
    score_dense_rank_term_sum_score_dense_rank
FROM windowed
ORDER BY score_dense_rank_term_sum_score_dense_rank
