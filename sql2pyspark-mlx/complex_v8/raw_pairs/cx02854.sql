WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score < 57.3
),
enriched_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        st.is_international AS student_is_international,
        st.student_name,
        st.enrolled_on AS student_enrolled_on,
        c.credits AS course_credits
    FROM base_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        student_is_international,
        YEAR(student_enrolled_on) AS student_enrolled_on_year,
        COUNT(*) AS row_count,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        COUNT(DISTINCT id) AS distinct_ids
    FROM enriched_student
    GROUP BY course_id, student_is_international, YEAR(student_enrolled_on)
)
SELECT
    distinct_student_ids,
    student_is_international,
    course_id,
    GROUPING(distinct_student_ids) AS is_total_distinct_student_ids,
    SUM(CASE WHEN student_is_international = TRUE THEN 1 ELSE 0 END) AS n_student_is_international_tru,
    SUM(distinct_ids) AS total_distinct_ids,
    ROUND(AVG(student_enrolled_on_year), 2) AS avg_student_enrolled_on_year
FROM stats
GROUP BY GROUPING SETS ((distinct_student_ids, student_is_international, course_id), (distinct_student_ids), (student_is_international), ())
