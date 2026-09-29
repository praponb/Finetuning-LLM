WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM enrollments
    WHERE student_id >= 4
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        student_id,
        term,
        SUM(score) AS total_score,
        COUNT(*) FILTER (WHERE term IN ('2024T2', '2024T1', '2025T1')) AS n_term_in_2024t2_2024t1_2025t1,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        COUNT(DISTINCT course_id) AS distinct_course_ids
    FROM prepared
    WHERE course_id >= 4
    GROUP BY course_id, student_id, term
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        course_id,
        student_id,
        term,
        total_score,
        n_term_in_2024t2_2024t1_2025t1,
        term_list,
        distinct_course_ids,
        LAST_VALUE(total_score) OVER (PARTITION BY student_id, distinct_course_ids ORDER BY n_term_in_2024t2_2024t1_2025t1, course_id, student_id, term ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_total_score,
        NTILE(10) OVER w_course_id_by_n_term_in_2024t AS n_term_in_2024t2_2024t1_2025t1_decile,
        LAST_VALUE(total_score) IGNORE NULLS OVER (PARTITION BY course_id ORDER BY n_term_in_2024t2_2024t1_2025t1, course_id, student_id, term ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_total_score,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_id)) AS distinct_term_in_course_id,
        COUNT(*) OVER w_course_id AS rows_in_course_id,
        MAX(distinct_course_ids) OVER (PARTITION BY n_term_in_2024t2_2024t1_2025t1, term ORDER BY n_term_in_2024t2_2024t1_2025t1 ASC, course_id, student_id, term ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS distinct_course_ids_max_6_prior
    FROM grouped
    WINDOW w_course_id_by_n_term_in_2024t AS (PARTITION BY course_id ORDER BY n_term_in_2024t2_2024t1_2025t1, course_id, student_id, term), w_course_id AS (PARTITION BY course_id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        distinct_term_in_course_id - LAG(distinct_term_in_course_id) OVER (PARTITION BY n_term_in_2024t2_2024t1_2025t1, distinct_term_in_course_id ORDER BY distinct_term_in_course_id DESC, course_id, student_id, term) AS distinct_term_in_course_id_change,
        FIRST_VALUE(last_known_total_score) OVER (PARTITION BY n_term_in_2024t2_2024t1_2025t1 ORDER BY n_term_in_2024t2_2024t1_2025t1_decile, course_id, student_id, term ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_known_total_score,
        ROUND(AVG(last_total_score) OVER (PARTITION BY n_term_in_2024t2_2024t1_2025t1 ORDER BY n_term_in_2024t2_2024t1_2025t1_decile, course_id, student_id, term ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS last_total_score_avg_2,
        ROUND(CUME_DIST() OVER (PARTITION BY n_term_in_2024t2_2024t1_2025t1 ORDER BY n_term_in_2024t2_2024t1_2025t1_decile), 3) AS n_term_in_2024t2_2024t1_2025t1_decile_cume_dist,
        FIRST_VALUE(last_total_score) IGNORE NULLS OVER (PARTITION BY n_term_in_2024t2_2024t1_2025t1, distinct_term_in_course_id ORDER BY distinct_term_in_course_id DESC, course_id, student_id, term ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_total_score,
        MAX(distinct_course_ids) OVER (PARTITION BY n_term_in_2024t2_2024t1_2025t1, distinct_term_in_course_id ORDER BY distinct_term_in_course_id DESC, course_id, student_id, term ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS distinct_course_ids_max_2_prior
    FROM ranked
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.course_id,
        s.student_id,
        s.term,
        s.n_term_in_2024t2_2024t1_2025t1,
        s.term_list,
        s.last_total_score,
        s.last_known_total_score,
        s.distinct_term_in_course_id,
        s.rows_in_course_id,
        s.distinct_course_ids_max_6_prior,
        s.distinct_term_in_course_id_change,
        s.last_total_score_avg_2,
        s.n_term_in_2024t2_2024t1_2025t1_decile_cume_dist,
        s.first_known_last_total_score,
        s.distinct_course_ids_max_2_prior,
        c.course_code,
        c.level AS course_level,
        st.gpa AS student_gpa,
        st.major AS student_major
    FROM metrics s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(student_major, '#', CAST(student_id AS STRING)) AS student_major_ref
    FROM with_details_course
    WHERE student_major <> 'Art'
)
SELECT
    student_id,
    term,
    distinct_term_in_course_id,
    COUNT(*) AS n_rows,
    SUM(n_term_in_2024t2_2024t1_2025t1_decile_cume_dist) FILTER (WHERE term IS NOT NULL) AS n_term_in_2024t2_2024t1_2025t1_decile_cume_dist_,
    COUNT(DISTINCT course_id) AS distinct_course_ids
FROM annotated
GROUP BY GROUPING SETS ((student_id, term, distinct_term_in_course_id), (student_id), (term))
