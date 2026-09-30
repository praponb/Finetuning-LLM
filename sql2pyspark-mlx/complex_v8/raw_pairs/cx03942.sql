WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 0.9, 2) AS score_adj
    FROM enrollments
    WHERE grade_status = 'INCOMPLETE'
),
combined_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        st.gpa AS student_gpa,
        st.major AS student_major,
        st.enrolled_on AS student_enrolled_on,
        c.level AS course_level,
        c.credits AS course_credits
    FROM base s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        student_enrolled_on,
        course_level,
        course_credits,
        ROUND(AVG(score) OVER (PARTITION BY course_level, student_major), 2) AS course_level_student_major_avg_score,
        MAX(score) OVER (ORDER BY course_credits NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_max_2_prior,
        COUNT(*) OVER (PARTITION BY course_level, student_major) AS rows_in_course_level_student_major,
        MAX(course_credits) OVER w_course_level AS course_level_max_course_credits,
        ROUND(score - AVG(score) OVER w_course_level, 2) AS score_vs_course_level_avg,
        LEAD(student_enrolled_on) OVER (PARTITION BY DATE_TRUNC('MONTH', student_enrolled_on), term ORDER BY student_enrolled_on NULLS LAST, id) AS next_student_enrolled_on,
        LAST_VALUE(score) OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        NTILE(5) OVER (PARTITION BY student_major, course_level ORDER BY student_gpa DESC NULLS FIRST, id) AS student_gpa_quintile,
        FIRST_VALUE(course_credits) OVER (ORDER BY course_credits ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_course_credits,
        RANK() OVER w_student_enrolled_on_month_te AS student_enrolled_on_rank,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_id)) AS distinct_student_id_in_course_id,
        LEAD(student_gpa, 1, 0) OVER (PARTITION BY TO_DATE(enrolled_at), course_id ORDER BY course_credits NULLS LAST, id) AS next_student_gpa,
        ROUND(CUME_DIST() OVER (ORDER BY course_credits NULLS LAST, id), 3) AS course_credits_cume_dist
    FROM combined_details
    WINDOW w_course_level AS (PARTITION BY course_level), w_student_enrolled_on_month_te AS (PARTITION BY DATE_TRUNC('MONTH', student_enrolled_on), term ORDER BY student_enrolled_on NULLS LAST, id)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        student_enrolled_on,
        course_level,
        course_credits,
        course_level_student_major_avg_score,
        score_max_2_prior,
        rows_in_course_level_student_major,
        course_level_max_course_credits,
        score_vs_course_level_avg,
        next_student_enrolled_on,
        last_score,
        student_gpa_quintile,
        first_course_credits,
        distinct_student_id_in_course_id,
        next_student_gpa,
        course_credits_cume_dist
    FROM ranked
    WHERE student_enrolled_on_rank <= 2
      AND score_max_2_prior < 195.0
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        term,
        student_enrolled_on,
        course_level,
        course_credits,
        course_level_student_major_avg_score,
        score_max_2_prior,
        course_level_max_course_credits,
        score_vs_course_level_avg,
        next_student_enrolled_on,
        last_score,
        student_gpa_quintile,
        distinct_student_id_in_course_id,
        next_student_gpa,
        course_credits_cume_dist,
        DENSE_RANK() OVER (ORDER BY course_credits, id) AS course_credits_dense_rank,
        ROUND(course_credits_cume_dist / NULLIF(SUM(course_credits_cume_dist) OVER (PARTITION BY DATE_TRUNC('MONTH', next_student_enrolled_on)), 0), 4) AS course_credits_cume_dist_share_of_next_student_e,
        SUM(first_course_credits) OVER (ORDER BY student_enrolled_on DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS first_course_credits_sum_2,
        ROUND(score_vs_course_level_avg - AVG(score_vs_course_level_avg) OVER (PARTITION BY student_id), 2) AS score_vs_course_level_avg_vs_student_id_avg,
        ROUND(PERCENT_RANK() OVER w_term_course_id_by_first_cour, 3) AS first_course_credits_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY course_credits, id), 3) AS course_credits_cume_dist_2
    FROM leaders
    WINDOW w_term_course_id_by_first_cour AS (PARTITION BY term, course_id ORDER BY first_course_credits)
),
grouped AS (
    -- Group-level totals
    SELECT
        first_course_credits_sum_2,
        ROUND(SUM(course_credits) / NULLIF(SUM(first_course_credits_percent_rank), 0), 3) AS course_credits_to_first_course_credits_percent_r,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        MIN(course_level_student_major_avg_score) AS min_course_level_student_major_avg_score,
        COUNT(*) FILTER (WHERE term = '2024T1') AS n_term_2024t1,
        ROUND(100 * COUNT(CASE WHEN term <> '2025T1' THEN 1 END) / COUNT(*), 1) AS pct_term_2025t1
    FROM windowed
    WHERE course_credits_cume_dist_2 <= 0.9
    GROUP BY first_course_credits_sum_2
)
-- Final projection
SELECT
    first_course_credits_sum_2,
    course_credits_to_first_course_credits_percent_r,
    distinct_student_ids,
    min_course_level_student_major_avg_score,
    ABS(distinct_student_ids - 6) AS distinct_student_ids_distance,
    LEAST(n_term_2024t1, distinct_student_ids) AS least_n_term_2024t1_distinct_student_ids
FROM grouped
