WITH cleaned_enrollments AS (
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
    WHERE TO_DATE(enrolled_at) < DATE '2025-02-06'
),
enriched_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        st.cohort AS student_cohort,
        st.gpa AS student_gpa
    FROM cleaned_enrollments s
    JOIN students st ON st.id = s.student_id AND st.major IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(enrolled_at) OVER (ORDER BY enrolled_at, id) AS prev_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY term, course_id ORDER BY enrolled_at, id), 3) AS enrolled_at_cume_dist,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY student_gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROW_NUMBER() OVER w_all_by_student_gpa AS seq_no,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2,
        DENSE_RANK() OVER (ORDER BY student_gpa) AS student_gpa_dense_rank,
        student_gpa - LAG(student_gpa) OVER w_all_by_student_gpa_2 AS student_gpa_change,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY term, course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_3,
        MAX(student_gpa) OVER (PARTITION BY course_id, student_id) AS course_id_student_id_max_student_gpa,
        SUM(student_gpa) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS student_gpa_sum_3_prior,
        MAX(student_gpa) OVER w_course_id AS course_id_max_student_gpa,
        LAG(enrolled_at) OVER (PARTITION BY term, course_id ORDER BY enrolled_at, id) AS prev_enrolled_at_2,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_id)) AS distinct_grade_status_in_course_id
    FROM enriched_student
    WINDOW w_all_by_student_gpa AS (ORDER BY student_gpa, id), w_all_by_student_gpa_2 AS (ORDER BY student_gpa DESC NULLS FIRST, id), w_course_id AS (PARTITION BY course_id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        student_gpa,
        prev_enrolled_at,
        enrolled_at_cume_dist,
        student_gpa_dense_rank,
        second_enrolled_at_3,
        course_id_student_id_max_student_gpa,
        course_id_max_student_gpa,
        prev_enrolled_at_2,
        distinct_grade_status_in_course_id,
        ROUND(PERCENT_RANK() OVER w_seq_no_by_second_enrolled_at, 3) AS second_enrolled_at_2_percent_rank,
        FIRST_VALUE(prev_enrolled_at_2) OVER (PARTITION BY student_cohort, student_id ORDER BY prev_enrolled_at_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prev_enrolled_at_2,
        ROUND(AVG(student_gpa_change) OVER (PARTITION BY MONTH(second_enrolled_at_2), YEAR(prev_enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS student_gpa_change_avg_3_prior,
        NTILE(5) OVER (PARTITION BY student_cohort, student_id ORDER BY prev_enrolled_at_2, id) AS prev_enrolled_at_2_quintile,
        LAST_VALUE(second_enrolled_at) IGNORE NULLS OVER (PARTITION BY seq_no ORDER BY second_enrolled_at_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_second_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(second_enrolled_at_2), YEAR(prev_enrolled_at) ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        MIN(enrolled_at_cume_dist) OVER (PARTITION BY MONTH(second_enrolled_at), student_cohort) AS second_enrolled_at_month_student_cohort_min_enro
    FROM with_trends
    WINDOW w_seq_no_by_second_enrolled_at AS (PARTITION BY seq_no ORDER BY second_enrolled_at_2 DESC)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(first_prev_enrolled_at_2) AS first_prev_enrolled_at_2_year,
        DATE_TRUNC('MONTH', second_enrolled_at_3) AS second_enrolled_at_3_month,
        course_id,
        MIN(prev_enrolled_at) AS first_prev_enrolled_at,
        COUNT(*) AS cnt,
        ROUND(SUM(course_id_student_id_max_student_gpa) / NULLIF(SUM(student_gpa_dense_rank), 0), 3) AS course_id_student_id_max_student_gpa_to_student_
    FROM sequenced
    GROUP BY YEAR(first_prev_enrolled_at_2), DATE_TRUNC('MONTH', second_enrolled_at_3), course_id
    HAVING COUNT(*) >= 2
)
SELECT DISTINCT course_id
FROM stats
ORDER BY course_id DESC
