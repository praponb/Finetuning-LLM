WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE (student_id IS NOT NULL OR score <= 85.2)
),
joined AS (
    -- Join students
    SELECT s.*, st.student_name, c.level AS course_level
    FROM recent s
    LEFT JOIN students st ON st.id = s.student_id AND st.is_international = FALSE
    LEFT JOIN courses c ON c.id = s.course_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(score) OVER (ORDER BY score, id) AS next_score,
        MIN(score) OVER (PARTITION BY course_level, grade_status) AS course_level_grade_status_min_score,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_id, student_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        ROUND(score - AVG(score) OVER w_course_level_grade_status, 2) AS score_vs_course_level_grade_status_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id, student_id ORDER BY score DESC, id), 3) AS score_cume_dist,
        FIRST_VALUE(score) IGNORE NULLS OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        LEAD(score, 3) OVER (PARTITION BY term ORDER BY score NULLS LAST, id) AS next3_score,
        score - LAG(score) OVER w_term_by_score AS score_change,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_level, grade_status), 0), 4) AS score_share_of_course_level_grade_status,
        SUM(score) OVER (PARTITION BY student_id, grade_status) AS student_id_grade_status_sum_score
    FROM joined
    WINDOW w_course_level_grade_status AS (PARTITION BY course_level, grade_status), w_term_by_score AS (PARTITION BY term ORDER BY score NULLS LAST, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        term,
        grade_status,
        enrolled_at_label,
        student_name,
        course_level,
        next_score,
        course_level_grade_status_min_score,
        second_score,
        score_vs_course_level_grade_status_avg,
        first_known_score,
        next3_score,
        score_share_of_course_level_grade_status,
        student_id_grade_status_sum_score,
        score_change - score AS score_change_minus_score,
        CASE WHEN score_cume_dist >= 0.8 THEN 'A' WHEN score_cume_dist >= 0.4 THEN 'B' ELSE 'C' END AS score_cume_dist_tier,
        ROUND(next3_score / NULLIF(next_score, 0), 3) AS next3_score_per_next_score
    FROM with_windows
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(second_score) OVER (PARTITION BY term ORDER BY course_level_grade_status_min_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_second_score,
        first_known_score - LAG(first_known_score) OVER (ORDER BY next3_score_per_next_score, id) AS first_known_score_change,
        LAG(next3_score) OVER (PARTITION BY term ORDER BY course_level_grade_status_min_score DESC, id) AS prev_next3_score
    FROM tagged
)
-- Final projection
SELECT
    id,
    term,
    grade_status,
    enrolled_at_label,
    course_level,
    course_level_grade_status_min_score,
    second_score,
    score_vs_course_level_grade_status_avg,
    first_known_score,
    next3_score,
    score_share_of_course_level_grade_status,
    student_id_grade_status_sum_score,
    score_change_minus_score,
    score_cume_dist_tier,
    running_second_score,
    first_known_score_change,
    prev_next3_score
FROM sequenced
ORDER BY score_change_minus_score DESC, running_second_score DESC
