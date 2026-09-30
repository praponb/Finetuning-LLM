WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE (student_id <= 15 OR term IS NOT NULL)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        MAX(score) OVER w_grade_status AS grade_status_max_score,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY grade_status)) AS distinct_course_id_in_grade_status
    FROM recent
    WINDOW w_grade_status AS (PARTITION BY grade_status)
),
joined_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.term,
        s.grade_status,
        s.grade_status_max_score,
        s.distinct_course_id_in_grade_status,
        st.is_international AS student_is_international
    FROM with_trends s
    LEFT JOIN students st ON st.id = s.student_id AND st.gpa BETWEEN 2.7 AND 3.4
),
joined_student_2 AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.term,
        s.grade_status,
        s.distinct_course_id_in_grade_status,
        s.student_is_international,
        st.gpa AS student_gpa,
        st.major AS student_major,
        st.is_international AS student_is_international_2
    FROM joined_student s
    JOIN students st ON st.id = s.student_id AND st.major IS NOT NULL
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY student_is_international_2) AS rows_in_student_is_international_2,
        ROUND(AVG(student_gpa) OVER (PARTITION BY grade_status, student_is_international ORDER BY student_gpa, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS student_gpa_avg_3,
        NTH_VALUE(student_gpa, 2) OVER (PARTITION BY grade_status, student_is_international ORDER BY student_gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_gpa,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY student_is_international_2)) AS distinct_term_in_student_is_international_2,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY student_is_international_2), 2) AS student_gpa_vs_student_is_international_2_avg
    FROM joined_student_2
)
SELECT
    id,
    student_id,
    term,
    grade_status,
    distinct_course_id_in_grade_status,
    student_gpa,
    student_major,
    rows_in_student_is_international_2,
    student_gpa_avg_3,
    second_student_gpa,
    distinct_term_in_student_is_international_2,
    student_gpa_vs_student_is_international_2_avg
FROM with_trends_2
ORDER BY term, distinct_term_in_student_is_international_2 DESC NULLS FIRST
