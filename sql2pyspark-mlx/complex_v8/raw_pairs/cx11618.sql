WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score > 11.3
),
with_parent AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        st.major AS student_major
    FROM valid_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        student_major,
        SIZE(COLLECT_SET(term) OVER w_student_major_student_id) AS distinct_term_in_student_major_student_id,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id, term), 0), 4) AS score_share_of_course_id_term,
        LAG(score) OVER (ORDER BY score DESC, id) AS prev_score,
        LEAD(score) OVER (ORDER BY score DESC, id) AS next_score,
        DENSE_RANK() OVER (ORDER BY score DESC, id) AS score_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_major, term ORDER BY score ASC), 3) AS score_percent_rank,
        score - LAG(score) OVER (ORDER BY score DESC, id) AS score_change,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY student_major, student_id)) AS distinct_course_id_in_student_major_student_id,
        SUM(score) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_major, student_id), 0), 4) AS score_share_of_student_major_student_id,
        ROUND(AVG(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        MIN(score) OVER (PARTITION BY course_id, term) AS course_id_term_min_score,
        COUNT(*) OVER (PARTITION BY student_major, student_id) AS rows_in_student_major_student_id,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY student_major ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        ROUND(score - AVG(score) OVER w_student_major_course_id, 2) AS score_vs_student_major_course_id_avg
    FROM with_parent
    WINDOW w_student_major_student_id AS (PARTITION BY student_major, student_id), w_student_major_course_id AS (PARTITION BY student_major, course_id)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM sequenced
    WHERE score_dense_rank <= 3
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(next_score / NULLIF(SUM(next_score) OVER (PARTITION BY distinct_term_in_student_major_student_id, term), 0), 4) AS next_score_share_of_distinct_term_in_student_maj,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term, course_id ORDER BY score_share_of_student_major_student_id ASC), 3) AS score_share_of_student_major_student_id_percent_,
        running_score - LAG(running_score) OVER (PARTITION BY student_major, course_id ORDER BY running_score NULLS LAST, id) AS running_score_change,
        ROUND(AVG(score_percent_rank) OVER w_distinct_term_in_student_maj, 2) AS distinct_term_in_student_major_student_id_term_a,
        COUNT(*) OVER (PARTITION BY term, course_id) AS rows_in_term_course_id,
        MIN(score_avg_3_prior) OVER (PARTITION BY term, course_id) AS term_course_id_min_score_avg_3_prior
    FROM leaders
    WINDOW w_distinct_term_in_student_maj AS (PARTITION BY distinct_term_in_student_major_student_id, term)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    distinct_term_in_student_major_student_id,
    score_share_of_course_id_term,
    next_score,
    score_dense_rank,
    score_percent_rank,
    score_change,
    distinct_course_id_in_student_major_student_id,
    running_score,
    score_share_of_student_major_student_id,
    score_avg_3_prior,
    course_id_term_min_score,
    rows_in_student_major_student_id,
    first_known_score,
    next_score_share_of_distinct_term_in_student_maj,
    running_score_change,
    rows_in_term_course_id,
    COALESCE(score, 0.0) AS score_filled,
    CONCAT(term, '#', CAST(rows_in_term_course_id AS STRING)) AS term_ref
FROM with_windows
