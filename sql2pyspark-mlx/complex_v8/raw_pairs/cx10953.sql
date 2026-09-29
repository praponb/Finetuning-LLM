WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status IS NOT NULL
      AND term <> '2024T2'
),
combined_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        st.student_name,
        st.enrolled_on AS student_enrolled_on
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY course_id, student_id ORDER BY student_enrolled_on DESC) AS student_enrolled_on_dense_rank,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        DENSE_RANK() OVER w_grade_status_by_enrolled_at_2 AS enrolled_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY enrolled_at ASC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        NTILE(10) OVER (ORDER BY enrolled_at ASC, id) AS enrolled_at_decile,
        RANK() OVER (ORDER BY enrolled_at ASC, id) AS enrolled_at_rank,
        DATEDIFF(student_enrolled_on, LAG(student_enrolled_on) OVER (PARTITION BY course_id ORDER BY student_enrolled_on, id)) AS days_since_prev_student_enrolled_on,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id, student_id ORDER BY student_enrolled_on DESC), 3) AS student_enrolled_on_percent_rank,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        NTILE(4) OVER w_grade_status_by_enrolled_at_2 AS enrolled_at_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status ORDER BY enrolled_at, id), 3) AS enrolled_at_cume_dist,
        NTH_VALUE(student_enrolled_on, 2) OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_enrolled_on,
        NTILE(5) OVER w_course_id_student_id_by_stud_2 AS student_enrolled_on_quintile
    FROM combined_details
    WINDOW w_grade_status_by_enrolled_at_2 AS (PARTITION BY grade_status ORDER BY enrolled_at, id), w_course_id_student_id_by_stud_2 AS (PARTITION BY course_id, student_id ORDER BY student_enrolled_on DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        student_name,
        student_enrolled_on_dense_rank,
        enrolled_at_dense_rank,
        minutes_since_prev_enrolled_at,
        enrolled_at_decile,
        days_since_prev_student_enrolled_on,
        student_enrolled_on_percent_rank,
        enrolled_at_cume_dist,
        second_student_enrolled_on,
        student_enrolled_on_quintile,
        MAX(student_enrolled_on_quintile) OVER (PARTITION BY course_id, enrolled_at_quartile) AS course_id_enrolled_at_quartile_max_student_enrol,
        ROUND((UNIX_TIMESTAMP(second_enrolled_at) - UNIX_TIMESTAMP(LAG(second_enrolled_at) OVER (PARTITION BY enrolled_at_decile, DATE_TRUNC('MONTH', second_student_enrolled_on) ORDER BY student_enrolled_on_percent_rank, id))) / 60, 1) AS minutes_since_prev_second_enrolled_at,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_id, enrolled_at_quartile)) AS distinct_student_id_in_course_id_enrolled_at_qua,
        ROUND(enrolled_at_dense_rank / NULLIF(SUM(enrolled_at_dense_rank) OVER w_course_id_enrolled_at_quarti, 0), 4) AS enrolled_at_dense_rank_share_of_course_id_enroll,
        ROUND(CUME_DIST() OVER (PARTITION BY enrolled_at_decile, DATE_TRUNC('MONTH', second_student_enrolled_on) ORDER BY student_enrolled_on_percent_rank), 3) AS student_enrolled_on_percent_rank_cume_dist,
        LAST_VALUE(last_known_enrolled_at) OVER (PARTITION BY enrolled_at_decile, DATE_TRUNC('MONTH', second_student_enrolled_on) ORDER BY student_enrolled_on_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_known_enrolled_at,
        ROUND(enrolled_at_rank / NULLIF(SUM(enrolled_at_rank) OVER (PARTITION BY course_id, enrolled_at_quartile), 0), 4) AS enrolled_at_rank_share_of_course_id_enrolled_at_
    FROM windowed
    WINDOW w_course_id_enrolled_at_quarti AS (PARTITION BY course_id, enrolled_at_quartile)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    enrolled_at,
    student_name,
    student_enrolled_on_dense_rank,
    enrolled_at_dense_rank,
    minutes_since_prev_enrolled_at,
    enrolled_at_decile,
    days_since_prev_student_enrolled_on,
    student_enrolled_on_percent_rank,
    enrolled_at_cume_dist,
    second_student_enrolled_on,
    student_enrolled_on_quintile,
    minutes_since_prev_second_enrolled_at,
    distinct_student_id_in_course_id_enrolled_at_qua,
    enrolled_at_dense_rank_share_of_course_id_enroll,
    enrolled_at_rank_share_of_course_id_enrolled_at_,
    DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at,
    DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at_2
FROM with_windows
ORDER BY enrolled_at_dense_rank NULLS LAST, id
LIMIT 20
