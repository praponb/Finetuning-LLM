WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT_WS(': ', grade_status, term) AS grade_status_term_label,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE (enrolled_at >= TIMESTAMP '2025-01-19 14:00:00' OR term = '2024T2')
),
combined_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.grade_status_term_label,
        s.grade_status_slug,
        st.student_name,
        st.gpa AS student_gpa
    FROM valid_enrollments s
    JOIN students st ON st.id = s.student_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(student_gpa / NULLIF(SUM(student_gpa) OVER (PARTITION BY grade_status, student_id), 0), 4) AS student_gpa_share_of_grade_status_student_id,
        MAX(score) OVER (PARTITION BY grade_status, student_id) AS grade_status_student_id_max_score,
        ROUND(AVG(student_gpa) OVER w_grade_status_student_id, 2) AS grade_status_student_id_avg_student_gpa,
        LAST_VALUE(student_gpa) OVER (ORDER BY student_gpa ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_student_gpa,
        ROUND(AVG(score) OVER w_student_id_grade_status, 2) AS student_id_grade_status_avg_score,
        ROUND(student_gpa / NULLIF(SUM(student_gpa) OVER (PARTITION BY student_id, grade_status), 0), 4) AS student_gpa_share_of_student_id_grade_status,
        MAX(student_gpa) OVER (PARTITION BY student_id, grade_status) AS student_id_grade_status_max_student_gpa
    FROM combined_details
    WINDOW w_grade_status_student_id AS (PARTITION BY grade_status, student_id), w_student_id_grade_status AS (PARTITION BY student_id, grade_status)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status_term_label,
        student_name,
        student_gpa,
        student_gpa_share_of_grade_status_student_id,
        last_student_gpa,
        student_gpa_share_of_student_id_grade_status,
        COUNT(*) OVER (PARTITION BY student_id ORDER BY grade_status_student_id_avg_student_gpa NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(5) OVER (PARTITION BY grade_status, student_id ORDER BY student_id_grade_status_max_student_gpa, id) AS student_id_grade_status_max_student_gpa_quintile,
        ROUND(score / NULLIF(SUM(score) OVER w_course_id_grade_status, 0), 4) AS score_share_of_course_id_grade_status,
        DENSE_RANK() OVER (PARTITION BY student_id ORDER BY grade_status_student_id_avg_student_gpa NULLS LAST, id) AS grade_status_student_id_avg_student_gpa_dense_ra,
        LAG(student_gpa, 2) OVER w_grade_status_student_id_by_s AS prev2_student_gpa,
        COUNT(*) OVER (PARTITION BY course_id, grade_status) AS rows_in_course_id_grade_status,
        ROUND(AVG(score) OVER (PARTITION BY course_id, grade_status), 2) AS course_id_grade_status_avg_score
    FROM with_trends
    WINDOW w_course_id_grade_status AS (PARTITION BY course_id, grade_status), w_grade_status_student_id_by_s AS (PARTITION BY grade_status, student_id ORDER BY student_id_grade_status_max_student_gpa, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        student_id,
        COUNT(*) AS n_rows,
        ROUND(SUM(score), 2) AS total_score,
        ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list
    FROM metrics
    WHERE grade_status_term_label IS NOT NULL
    GROUP BY 1
)
SELECT
    student_id,
    n_rows,
    total_score,
    student_name_list
FROM summary
WHERE SIZE(student_name_list) >= 1
ORDER BY total_score DESC NULLS FIRST, student_id
LIMIT 10
