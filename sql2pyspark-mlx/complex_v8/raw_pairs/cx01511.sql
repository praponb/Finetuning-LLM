WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.5, 2) AS score_adj,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE term NOT IN ('2025T1', '2024T2')
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.score_adj,
        s.term_slug,
        c.course_code,
        c.level AS course_level,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort
    FROM valid s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(student_gpa) OVER (PARTITION BY grade_status, student_cohort) AS grade_status_student_cohort_min_student_gpa,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_cohort ORDER BY enrolled_at ASC, id), 3) AS enrolled_at_percent_rank,
        SUM(student_gpa) OVER (PARTITION BY course_level, student_cohort) AS course_level_student_cohort_sum_student_gpa,
        ROUND(score_adj - AVG(score_adj) OVER (PARTITION BY course_level, student_cohort), 2) AS score_adj_vs_course_level_student_cohort_avg,
        MIN(score_adj) OVER (PARTITION BY course_level, student_cohort) AS course_level_student_cohort_min_score_adj,
        ROUND(CUME_DIST() OVER (PARTITION BY course_level, grade_status ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(enrolled_at), student_cohort ORDER BY student_gpa) AS student_gpa_dense_rank,
        MAX(student_gpa) OVER (PARTITION BY course_level, grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS student_gpa_max_3
    FROM with_parent_course
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        score_adj,
        term_slug,
        course_code,
        course_level,
        student_gpa,
        student_cohort,
        enrolled_at_percent_rank,
        course_level_student_cohort_sum_student_gpa,
        score_adj_vs_course_level_student_cohort_avg,
        course_level_student_cohort_min_score_adj,
        enrolled_at_cume_dist,
        student_gpa_dense_rank,
        student_gpa_max_3,
        NTILE(10) OVER (ORDER BY score_adj_vs_course_level_student_cohort_avg ASC NULLS LAST, id) AS score_adj_vs_course_level_student_cohort_avg_dec,
        LAG(student_gpa_max_3) OVER (ORDER BY score_adj, id) AS prev_student_gpa_max_3,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY student_id, course_id), 2) AS student_gpa_vs_student_id_course_id_avg
    FROM sequenced
),
stats AS (
    -- Group-level totals
    SELECT
        student_id,
        course_level,
        SUM(score_adj_vs_course_level_student_cohort_avg_dec) AS total_score_adj_vs_course_level_student_cohort_a,
        COUNT(*) AS row_count,
        ROUND(SUM(student_gpa_max_3) / NULLIF(SUM(enrolled_at_percent_rank), 0), 3) AS student_gpa_max_3_to_enrolled_at_percent_rank_ra,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(course_level_student_cohort_sum_student_gpa) / NULLIF(SUM(enrolled_at_percent_rank), 0), 3) AS course_level_student_cohort_sum_student_gpa_to_e
    FROM with_trends
    GROUP BY student_id, course_level
    HAVING COUNT(*) >= 2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY student_id ORDER BY course_level_student_cohort_sum_student_gpa_to_e, student_id, course_level) AS course_level_student_cohort_sum_student_gpa_to_e_2,
        NTILE(10) OVER (PARTITION BY student_id ORDER BY course_level_student_cohort_sum_student_gpa_to_e, student_id, course_level) AS course_level_student_cohort_sum_student_gpa_to_e_3,
        LAST_VALUE(distinct_ids) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY course_level_student_cohort_sum_student_gpa_to_e, student_id, course_level ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_distinct_ids
    FROM stats
)
SELECT
    student_id,
    course_level,
    total_score_adj_vs_course_level_student_cohort_a,
    student_gpa_max_3_to_enrolled_at_percent_rank_ra,
    distinct_ids,
    course_level_student_cohort_sum_student_gpa_to_e,
    course_level_student_cohort_sum_student_gpa_to_e_2,
    last_known_distinct_ids
FROM windowed
ORDER BY total_score_adj_vs_course_level_student_cohort_a, distinct_ids
