WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT *
    FROM enrollments
    WHERE (student_id <= 11 OR score >= 36.6)
),
joined AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        st.gpa AS student_gpa,
        st.major AS student_major
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_course_id_student_id_by_stud AS student_gpa_rank,
        NTILE(10) OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at ASC, id) AS enrolled_at_decile,
        COUNT(*) OVER (PARTITION BY MONTH(enrolled_at), student_id) AS rows_in_enrolled_at_month_student_id,
        LEAD(student_gpa) OVER w_all_by_student_gpa AS next_student_gpa,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY MONTH(enrolled_at), student_id)) AS distinct_course_id_in_enrolled_at_month_student_,
        ROUND(AVG(student_gpa) OVER (PARTITION BY course_id, student_id ORDER BY student_gpa, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS student_gpa_avg_2_prior,
        RANK() OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at ASC) AS enrolled_at_rank,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY MONTH(enrolled_at), student_id), 2) AS student_gpa_vs_enrolled_at_month_student_id_avg,
        RANK() OVER w_all_by_student_gpa AS student_gpa_rank_2
    FROM joined
    WINDOW w_course_id_student_id_by_stud AS (PARTITION BY course_id, student_id ORDER BY student_gpa), w_all_by_student_gpa AS (ORDER BY student_gpa DESC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at_decile,
        rows_in_enrolled_at_month_student_id,
        next_student_gpa,
        distinct_course_id_in_enrolled_at_month_student_,
        student_gpa_avg_2_prior,
        enrolled_at_rank,
        student_gpa_rank_2,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM with_trends
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(next_student_gpa) OVER (PARTITION BY enrolled_at_quarter), 2) AS enrolled_at_quarter_avg_next_student_gpa,
        LEAD(student_gpa_avg_2_prior) OVER (PARTITION BY enrolled_at_quarter ORDER BY next_student_gpa ASC, id) AS next_student_gpa_avg_2_prior,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY enrolled_at_quarter)) AS distinct_course_id_in_enrolled_at_quarter,
        DENSE_RANK() OVER (PARTITION BY enrolled_at_quarter ORDER BY next_student_gpa ASC, id) AS next_student_gpa_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY distinct_course_id_in_enrolled_at_month_student_ ASC), 3) AS distinct_course_id_in_enrolled_at_month_student__2,
        MAX(student_gpa_avg_2_prior) OVER (PARTITION BY enrolled_at_quarter ORDER BY next_student_gpa ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_student_gpa_avg_2_prior_so_far,
        MIN(enrolled_at_quarter) OVER (PARTITION BY enrolled_at_rank, course_id) AS enrolled_at_rank_course_id_min_enrolled_at_quart
    FROM annotated
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    enrolled_at_decile,
    rows_in_enrolled_at_month_student_id,
    next_student_gpa,
    enrolled_at_rank,
    enrolled_at_quarter_avg_next_student_gpa,
    next_student_gpa_avg_2_prior,
    distinct_course_id_in_enrolled_at_quarter,
    next_student_gpa_dense_rank,
    distinct_course_id_in_enrolled_at_month_student__2,
    max_student_gpa_avg_2_prior_so_far,
    enrolled_at_rank_course_id_min_enrolled_at_quart,
    enrolled_at_quarter - student_gpa_rank_2 AS enrolled_at_quarter_minus_student_gpa_rank_2
FROM with_trends_2
