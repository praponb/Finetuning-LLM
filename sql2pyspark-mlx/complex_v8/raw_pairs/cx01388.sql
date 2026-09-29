WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 08:00:00'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.grade_status_term_label,
        st.gpa AS student_gpa
    FROM prepared s
    LEFT JOIN students st ON st.id = s.student_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY student_id, term) AS rows_in_student_id_term,
        student_gpa - LAG(student_gpa) OVER (PARTITION BY MONTH(enrolled_at), student_id ORDER BY enrolled_at DESC, id) AS student_gpa_change,
        LAST_VALUE(student_gpa) OVER (PARTITION BY course_id ORDER BY student_gpa ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_student_gpa,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY term, course_id), 2) AS student_gpa_vs_term_course_id_avg,
        RANK() OVER w_enrolled_at_month_student_id_2 AS enrolled_at_rank,
        MAX(student_gpa) OVER (PARTITION BY student_id, term) AS student_id_term_max_student_gpa,
        ROUND(student_gpa / NULLIF(SUM(student_gpa) OVER (PARTITION BY student_id, term), 0), 4) AS student_gpa_share_of_student_id_term,
        DENSE_RANK() OVER w_term_by_student_gpa AS student_gpa_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY student_id, term ORDER BY student_gpa NULLS LAST, id) AS seq_no,
        NTILE(5) OVER (PARTITION BY course_id ORDER BY student_gpa ASC, id) AS student_gpa_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id ORDER BY student_gpa ASC), 3) AS student_gpa_cume_dist,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_id, term ORDER BY student_gpa NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY term), 2) AS student_gpa_vs_term_avg
    FROM combined_details
    WINDOW w_enrolled_at_month_student_id_2 AS (PARTITION BY MONTH(enrolled_at), student_id ORDER BY enrolled_at DESC), w_student_id_term AS (PARTITION BY student_id, term), w_term_by_student_gpa AS (PARTITION BY term ORDER BY student_gpa DESC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        term,
        enrolled_at,
        student_gpa,
        rows_in_student_id_term,
        student_gpa_change,
        student_gpa_vs_term_course_id_avg,
        enrolled_at_rank,
        student_gpa_share_of_student_id_term,
        student_gpa_dense_rank,
        student_gpa_quintile,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM metrics
    WHERE student_gpa < 3.3
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        term,
        enrolled_at,
        student_gpa,
        student_gpa_change,
        student_gpa_vs_term_course_id_avg,
        enrolled_at_rank,
        student_gpa_dense_rank,
        student_gpa_quintile,
        days_since_enrolled_at,
        ROUND(rows_in_student_id_term / NULLIF(SUM(rows_in_student_id_term) OVER w_student_gpa_quintile_student, 0), 4) AS rows_in_student_id_term_share_of_student_gpa_qui,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY student_gpa_quintile, student_id), 2) AS student_gpa_vs_student_gpa_quintile_student_id_a,
        LAST_VALUE(student_gpa_dense_rank) IGNORE NULLS OVER (ORDER BY student_gpa_vs_term_course_id_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_student_gpa_dense_rank,
        COUNT(*) OVER (PARTITION BY student_gpa_quintile, student_id) AS rows_in_student_gpa_quintile_student_id,
        SUM(student_gpa_change) OVER (ORDER BY enrolled_at_rank ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_student_gpa_change,
        ROUND(AVG(enrolled_at_rank) OVER (ORDER BY enrolled_at_rank ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS enrolled_at_rank_avg_1_prior,
        MAX(student_gpa_quintile) OVER (ORDER BY student_gpa_vs_term_course_id_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_student_gpa_quintile_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY student_gpa_quintile ORDER BY days_since_enrolled_at), 3) AS days_since_enrolled_at_cume_dist
    FROM labeled
    WINDOW w_student_gpa_quintile_student AS (PARTITION BY student_gpa_quintile, student_id)
)
SELECT
    id,
    term,
    enrolled_at,
    student_gpa,
    student_gpa_change,
    student_gpa_vs_term_course_id_avg,
    enrolled_at_rank,
    student_gpa_dense_rank,
    student_gpa_quintile,
    days_since_enrolled_at,
    student_gpa_vs_student_gpa_quintile_student_id_a,
    last_known_student_gpa_dense_rank,
    rows_in_student_gpa_quintile_student_id,
    running_student_gpa_change,
    days_since_enrolled_at_cume_dist
FROM with_trends
WHERE student_gpa_change > -0.8
ORDER BY student_gpa_change DESC NULLS FIRST
