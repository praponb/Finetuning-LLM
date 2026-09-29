WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        CASE WHEN score >= 74.9 THEN 'HIGH' WHEN score >= 38.1 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE term IN ('2025T1', '2024T2', '2024T1')
      AND course_id IS NOT NULL
),
joined AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_tier,
        st.student_name,
        st.gpa AS student_gpa,
        c.level AS course_level
    FROM scoped s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(score) OVER w_term_score_tier AS term_score_tier_sum_score,
        ROUND(score - AVG(score) OVER w_course_id_course_level, 2) AS score_vs_course_id_course_level_avg,
        NTH_VALUE(student_gpa, 2) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_gpa,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY score ASC, id) AS score_dense_rank,
        NTILE(4) OVER (PARTITION BY student_id ORDER BY student_gpa DESC NULLS FIRST, id) AS student_gpa_quartile,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        ROUND(AVG(student_gpa) OVER (PARTITION BY course_id ORDER BY score ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS student_gpa_avg_3_prior,
        ROUND(AVG(score) OVER (PARTITION BY course_id ORDER BY score ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_avg_2,
        RANK() OVER w_all_by_student_gpa_ties AS student_gpa_rank,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY student_id ORDER BY student_gpa DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        SUM(score) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_sum_7,
        ROUND(AVG(student_gpa) OVER (PARTITION BY course_id ORDER BY score ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS student_gpa_avg_4
    FROM joined
    WINDOW w_term_score_tier AS (PARTITION BY term, score_tier), w_course_id_course_level AS (PARTITION BY course_id, course_level), w_all_by_student_gpa_ties AS (ORDER BY student_gpa DESC)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        score_tier,
        student_name,
        student_gpa,
        course_level,
        term_score_tier_sum_score,
        score_vs_course_id_course_level_avg,
        second_student_gpa,
        student_gpa_quartile,
        last_known_score,
        student_gpa_avg_3_prior,
        score_avg_2,
        student_gpa_rank,
        second_enrolled_at,
        score_sum_7,
        student_gpa_avg_4
    FROM sequenced
    WHERE score_dense_rank <= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_score_tier_by_second_student AS second_student_gpa_dense_rank,
        ROUND(student_gpa - AVG(student_gpa) OVER w_score_tier_student_id, 2) AS student_gpa_vs_score_tier_student_id_avg,
        MAX(last_known_score) OVER (PARTITION BY course_level, MONTH(second_enrolled_at) ORDER BY student_gpa_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_last_known_score_so_far,
        ROUND(CUME_DIST() OVER w_all_by_enrolled_at, 3) AS enrolled_at_cume_dist,
        SUM(student_gpa_avg_3_prior) OVER (PARTITION BY score_tier, student_id) AS score_tier_student_id_sum_student_gpa_avg_3_prio
    FROM leaders
    WINDOW w_score_tier_by_second_student AS (PARTITION BY score_tier ORDER BY second_student_gpa DESC), w_score_tier_student_id AS (PARTITION BY score_tier, student_id), w_all_by_enrolled_at AS (ORDER BY enrolled_at, id)
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    enrolled_at,
    score_tier,
    student_name,
    student_gpa,
    course_level,
    term_score_tier_sum_score,
    second_student_gpa,
    student_gpa_quartile,
    last_known_score,
    student_gpa_avg_3_prior,
    score_avg_2,
    second_enrolled_at,
    score_sum_7,
    student_gpa_avg_4,
    student_gpa_vs_score_tier_student_id_avg,
    max_last_known_score_so_far,
    enrolled_at_cume_dist,
    score_tier_student_id_sum_student_gpa_avg_3_prio
FROM windowed
ORDER BY student_gpa_vs_score_tier_student_id_avg, student_gpa DESC
