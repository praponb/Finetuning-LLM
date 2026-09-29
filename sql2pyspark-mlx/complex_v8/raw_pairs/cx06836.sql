WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE term IN ('2024T1', '2024T2', '2025T1')
),
with_parent AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at_label,
        s.score_filled,
        c.level AS course_level,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa
    FROM filtered_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id AND c.level IS NOT NULL
    JOIN students st ON st.id = s.student_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(student_gpa, 2) OVER (PARTITION BY term ORDER BY score ASC NULLS LAST, id) AS prev2_student_gpa,
        ROUND(CUME_DIST() OVER (PARTITION BY course_level ORDER BY score NULLS LAST), 3) AS score_cume_dist,
        COUNT(*) OVER (PARTITION BY student_is_international) AS rows_in_student_is_international,
        ROUND(score - AVG(score) OVER w_student_is_international, 2) AS score_vs_student_is_international_avg,
        ROUND(CUME_DIST() OVER w_term_by_score_filled_ties, 3) AS score_filled_cume_dist,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY term ORDER BY score ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        SUM(score_filled) OVER (PARTITION BY student_is_international) AS student_is_international_sum_score_filled,
        NTILE(10) OVER w_course_level_by_score AS score_decile,
        LEAD(score, 1, 0) OVER w_term_by_score AS next_score,
        ROW_NUMBER() OVER (PARTITION BY term ORDER BY score_filled DESC, id) AS score_filled_seq,
        DENSE_RANK() OVER w_course_level_by_score_ties AS score_dense_rank,
        SUM(student_gpa) OVER (PARTITION BY term ORDER BY score ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS student_gpa_sum_2,
        SUM(score) OVER (PARTITION BY student_is_international) AS student_is_international_sum_score
    FROM with_parent
    WINDOW w_student_is_international AS (PARTITION BY student_is_international), w_term_by_score_filled_ties AS (PARTITION BY term ORDER BY score_filled DESC), w_course_level_by_score AS (PARTITION BY course_level ORDER BY score NULLS LAST, id), w_term_by_score AS (PARTITION BY term ORDER BY score ASC NULLS LAST, id), w_course_level_by_score_ties AS (PARTITION BY course_level ORDER BY score NULLS LAST)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE score_filled_seq <= 5
      AND student_id IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(student_gpa_sum_2) OVER (PARTITION BY score_filled_seq, grade_status ORDER BY score_vs_student_is_international_avg, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS student_gpa_sum_2_sum_4,
        NTILE(3) OVER (PARTITION BY score_filled_seq, grade_status ORDER BY score_vs_student_is_international_avg, id) AS score_vs_student_is_international_avg_tercile,
        SUM(score_filled_cume_dist) OVER (PARTITION BY score_dense_rank, student_id) AS score_dense_rank_student_id_sum_score_filled_cum,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY score_dense_rank, student_id), 0), 4) AS score_share_of_score_dense_rank_student_id
    FROM top_rows
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at_label,
    course_level,
    student_gpa,
    prev2_student_gpa,
    score_cume_dist,
    rows_in_student_is_international,
    score_vs_student_is_international_avg,
    score_filled_cume_dist,
    first_known_score,
    student_is_international_sum_score_filled,
    score_decile,
    next_score,
    score_dense_rank,
    student_gpa_sum_2,
    student_is_international_sum_score,
    student_gpa_sum_2_sum_4,
    score_vs_student_is_international_avg_tercile,
    score_dense_rank_student_id_sum_score_filled_cum,
    score_share_of_score_dense_rank_student_id,
    next_score + score_decile AS next_score_plus_score_decile
FROM ranked
