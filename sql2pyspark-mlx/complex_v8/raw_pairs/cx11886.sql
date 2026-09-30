WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        MONTH(enrolled_at) AS enrolled_at_month,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE course_id >= 11
),
with_parent AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at_month,
        s.days_since_enrolled_at,
        st.is_international AS student_is_international,
        st.enrolled_on AS student_enrolled_on,
        c.dept AS course_dept
    FROM prepared s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at_month,
        days_since_enrolled_at,
        student_is_international,
        student_enrolled_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_dept, enrolled_at_month ORDER BY days_since_enrolled_at DESC NULLS FIRST, id), 3) AS days_since_enrolled_at_percent_rank,
        DATEDIFF(student_enrolled_on, LAG(student_enrolled_on) OVER w_student_is_international_by) AS days_since_prev_student_enrolled_on,
        ROUND(enrolled_at_month / NULLIF(SUM(enrolled_at_month) OVER (PARTITION BY YEAR(student_enrolled_on), enrolled_at_month), 0), 4) AS enrolled_at_month_share_of_student_enrolled_on_y,
        ROW_NUMBER() OVER (PARTITION BY student_is_international ORDER BY days_since_enrolled_at, id) AS days_since_enrolled_at_seq,
        COUNT(*) OVER (PARTITION BY student_is_international ORDER BY days_since_enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY term)) AS distinct_course_id_in_term,
        MAX(days_since_enrolled_at) OVER (PARTITION BY student_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_days_since_enrolled_at_so_far,
        RANK() OVER (PARTITION BY student_id ORDER BY score DESC) AS score_rank,
        COUNT(*) OVER w_term AS rows_in_term,
        LEAD(enrolled_at_month) OVER w_term_course_dept_by_enrolled AS next_enrolled_at_month,
        COUNT(*) OVER (PARTITION BY YEAR(student_enrolled_on), enrolled_at_month) AS rows_in_student_enrolled_on_year_enrolled_at_mon
    FROM with_parent
    WINDOW w_student_is_international_by AS (PARTITION BY student_is_international ORDER BY days_since_enrolled_at, id), w_term AS (PARTITION BY term), w_term_course_dept_by_enrolled AS (PARTITION BY term, course_dept ORDER BY enrolled_at_month, id)
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at_month,
        days_since_enrolled_at,
        student_is_international,
        student_enrolled_on,
        days_since_enrolled_at_percent_rank,
        days_since_prev_student_enrolled_on,
        enrolled_at_month_share_of_student_enrolled_on_y,
        days_since_enrolled_at_seq,
        running_count,
        distinct_course_id_in_term,
        max_days_since_enrolled_at_so_far,
        score_rank,
        next_enrolled_at_month,
        rows_in_student_enrolled_on_year_enrolled_at_mon,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_course_id_in_term ORDER BY rows_in_student_enrolled_on_year_enrolled_at_mon ASC, id), 3) AS rows_in_student_enrolled_on_year_enrolled_at_mon_2,
        NTH_VALUE(student_enrolled_on, 2) OVER (PARTITION BY distinct_course_id_in_term ORDER BY rows_in_student_enrolled_on_year_enrolled_at_mon ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_enrolled_on,
        ROUND(CUME_DIST() OVER (ORDER BY student_enrolled_on), 3) AS student_enrolled_on_cume_dist,
        SIZE(COLLECT_SET(term) OVER w_student_is_international) AS distinct_term_in_student_is_international
    FROM with_windows
    WINDOW w_student_is_international AS (PARTITION BY student_is_international)
)
SELECT
    id,
    student_id,
    term,
    grade_status,
    days_since_enrolled_at,
    days_since_enrolled_at_percent_rank,
    days_since_prev_student_enrolled_on,
    enrolled_at_month_share_of_student_enrolled_on_y,
    days_since_enrolled_at_seq,
    running_count,
    distinct_course_id_in_term,
    max_days_since_enrolled_at_so_far,
    score_rank,
    next_enrolled_at_month,
    rows_in_student_enrolled_on_year_enrolled_at_mon_2,
    second_student_enrolled_on,
    distinct_term_in_student_is_international
FROM with_windows_2
ORDER BY running_count, max_days_since_enrolled_at_so_far
