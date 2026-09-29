WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        CASE WHEN grade_status IN ('INCOMPLETE', 'PASS') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE term IN ('2024T2', '2025T1', '2024T1')
),
combined_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.enrolled_at_label,
        s.grade_status_group,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa
    FROM base s
    JOIN students st ON st.id = s.student_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(student_gpa, 2, 0) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS next2_student_gpa,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY grade_status_group), 2) AS student_gpa_vs_grade_status_group_avg,
        LAG(student_gpa, 3) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id) AS prev3_student_gpa,
        RANK() OVER w_all_by_student_gpa_ties AS student_gpa_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id ORDER BY enrolled_at DESC), 3) AS enrolled_at_percent_rank,
        LAG(student_gpa) OVER (PARTITION BY course_id, student_id ORDER BY student_gpa, id) AS prev_student_gpa,
        MAX(student_gpa) OVER (PARTITION BY course_id, student_id ORDER BY student_gpa, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS student_gpa_max_2,
        DENSE_RANK() OVER (ORDER BY student_gpa DESC, id) AS student_gpa_dense_rank
    FROM combined_details_student
    WINDOW w_all_by_student_gpa_ties AS (ORDER BY student_gpa DESC)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at,
        grade_status_group,
        student_gpa,
        next2_student_gpa,
        student_gpa_vs_grade_status_group_avg,
        prev3_student_gpa,
        student_gpa_rank,
        enrolled_at_percent_rank,
        prev_student_gpa,
        student_gpa_max_2,
        student_gpa_dense_rank,
        RANK() OVER (PARTITION BY student_gpa_rank, course_id ORDER BY prev_student_gpa DESC, id) AS prev_student_gpa_rank,
        RANK() OVER w_all_by_enrolled_at_ties AS enrolled_at_rank,
        COUNT(*) OVER w_student_gpa_dense_rank_grade AS rows_in_student_gpa_dense_rank_grade_status_grou,
        RANK() OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS enrolled_at_rank_2
    FROM metrics
    WINDOW w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at DESC NULLS FIRST), w_student_gpa_dense_rank_grade AS (PARTITION BY student_gpa_dense_rank, grade_status_group)
)
-- Final projection
SELECT
    id,
    student_id,
    enrolled_at,
    grade_status_group,
    student_gpa,
    next2_student_gpa,
    prev3_student_gpa,
    student_gpa_rank,
    prev_student_gpa,
    student_gpa_max_2,
    student_gpa_dense_rank,
    prev_student_gpa_rank,
    enrolled_at_rank,
    enrolled_at_rank_2,
    ROUND(enrolled_at_percent_rank * 1.1, 2) AS enrolled_at_percent_rank_adj
FROM windowed
ORDER BY id DESC NULLS FIRST
LIMIT 20
