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
        UPPER(TRIM(grade_status)) AS grade_status_clean,
        SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix
    FROM enrollments
    WHERE term NOT IN ('2024T2', '2025T2')
      AND course_id IS NOT NULL
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
        s.grade_status_clean,
        c.dept AS course_dept,
        c.level AS course_level,
        c.credits AS course_credits,
        st.student_name
    FROM filtered_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER (ORDER BY score DESC, id) AS score_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status, term ORDER BY score DESC, id), 3) AS score_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY student_id, course_dept ORDER BY enrolled_at DESC, id) AS enrolled_at_seq,
        COUNT(*) OVER (PARTITION BY term ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(course_credits) OVER (PARTITION BY student_id, course_level) AS student_id_course_level_min_course_credits,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER w_student_id_course_level, 0), 4) AS course_credits_share_of_student_id_course_level,
        course_credits - LAG(course_credits) OVER w_grade_status_term_by_score AS course_credits_change,
        ROUND(AVG(course_credits) OVER w_term, 2) AS term_avg_course_credits,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER (PARTITION BY term), 0), 4) AS course_credits_share_of_term,
        COUNT(*) OVER w_student_id_course_level AS rows_in_student_id_course_level,
        ROUND(score - AVG(score) OVER (PARTITION BY term), 2) AS score_vs_term_avg,
        MIN(course_credits) OVER (PARTITION BY grade_status, term ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_course_credits_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status, term ORDER BY score DESC, id), 3) AS score_percent_rank
    FROM with_details
    WINDOW w_student_id_course_level AS (PARTITION BY student_id, course_level), w_grade_status_term_by_score AS (PARTITION BY grade_status, term ORDER BY score DESC, id), w_term AS (PARTITION BY term)
),
totals AS (
    -- Summarise per group
    SELECT
        course_credits_change,
        CASE WHEN score_percent_rank >= 0.6 THEN 'large' WHEN score_percent_rank >= 0.2 THEN 'medium' ELSE 'small' END AS score_percent_rank_tier,
        enrolled_at_seq,
        ROUND(STDDEV(term_avg_course_credits), 2) AS stddev_term_avg_course_credits,
        COUNT(*) FILTER (WHERE LENGTH(TRIM(student_name)) >= 8) AS n_length_trim_student_name_8,
        SUM(course_credits) AS total_course_credits,
        ROUND(SUM(course_credits_share_of_term) / NULLIF(SUM(student_id_course_level_min_course_credits), 0), 3) AS course_credits_share_of_term_to_student_id_cours,
        SUM(CASE WHEN LENGTH(TRIM(student_name)) >= 5 THEN 1 ELSE 0 END) AS n_length_trim_student_name_5
    FROM windowed
    GROUP BY course_credits_change, CASE WHEN score_percent_rank >= 0.6 THEN 'large' WHEN score_percent_rank >= 0.2 THEN 'medium' ELSE 'small' END, enrolled_at_seq
)
-- Final projection
SELECT
    course_credits_change,
    score_percent_rank_tier,
    enrolled_at_seq,
    stddev_term_avg_course_credits,
    n_length_trim_student_name_8,
    total_course_credits,
    course_credits_share_of_term_to_student_id_cours,
    n_length_trim_student_name_5
FROM totals
WHERE course_credits_change <= 1
ORDER BY total_course_credits, n_length_trim_student_name_8 DESC
