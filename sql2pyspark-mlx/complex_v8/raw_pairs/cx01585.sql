WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-02-06 09:00:00'
      AND student_id >= 13
),
joined_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.term_grade_status_label,
        s.enrolled_at_date,
        st.cohort AS student_cohort,
        st.student_name,
        c.level AS course_level
    FROM scoped s
    LEFT JOIN students st ON st.id = s.student_id AND st.cohort = 'C2024'
    LEFT JOIN courses c ON c.id = s.course_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        term,
        grade_status,
        enrolled_at,
        term_grade_status_label,
        student_name,
        course_level,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_course_id_grade_status_by_en)) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at_date), 3) AS enrolled_at_date_cume_dist,
        LEAD(enrolled_at, 2) OVER (PARTITION BY YEAR(enrolled_at_date), course_id ORDER BY enrolled_at_date DESC, id) AS next2_enrolled_at,
        NTILE(3) OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at_date, id) AS enrolled_at_date_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_level ORDER BY enrolled_at_date DESC, id), 3) AS enrolled_at_date_percent_rank,
        ROUND(PERCENT_RANK() OVER w_course_id_grade_status_by_en_2, 3) AS enrolled_at_date_percent_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(enrolled_at_date), course_id ORDER BY enrolled_at_date DESC, id), 3) AS enrolled_at_date_percent_rank_3,
        ROW_NUMBER() OVER w_course_id_grade_status_by_en AS seq_no,
        LAG(enrolled_at) OVER (PARTITION BY YEAR(enrolled_at_date), course_id ORDER BY enrolled_at_date DESC, id) AS prev_enrolled_at,
        LAG(enrolled_at, 2) OVER (PARTITION BY MONTH(enrolled_at_date) ORDER BY enrolled_at_date, id) AS prev2_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY YEAR(enrolled_at_date), course_id ORDER BY enrolled_at_date DESC, id) AS enrolled_at_date_dense_rank,
        LAG(enrolled_at_date, 2) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at_date, id) AS prev2_enrolled_at_date,
        NTH_VALUE(enrolled_at_date, 2) OVER (PARTITION BY YEAR(enrolled_at_date), course_id ORDER BY enrolled_at_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_date
    FROM joined_student
    WINDOW w_course_id_grade_status_by_en AS (PARTITION BY course_id, grade_status ORDER BY enrolled_at_date, id), w_course_id_grade_status_by_en_2 AS (PARTITION BY course_id, grade_status ORDER BY enrolled_at_date)
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        term,
        grade_status,
        enrolled_at,
        term_grade_status_label,
        student_name,
        course_level,
        minutes_since_prev_enrolled_at,
        enrolled_at_date_cume_dist,
        next2_enrolled_at,
        enrolled_at_date_percent_rank,
        enrolled_at_date_percent_rank_2,
        enrolled_at_date_percent_rank_3,
        seq_no,
        prev_enrolled_at,
        prev2_enrolled_at,
        prev2_enrolled_at_date,
        DATEDIFF(second_enrolled_at_date, LAG(second_enrolled_at_date) OVER (PARTITION BY DATE_TRUNC('MONTH', second_enrolled_at_date) ORDER BY prev2_enrolled_at_date, id)) AS days_since_prev_second_enrolled_at_date,
        ROUND(enrolled_at_date_tercile / NULLIF(SUM(enrolled_at_date_tercile) OVER (PARTITION BY enrolled_at_date_dense_rank), 0), 4) AS enrolled_at_date_tercile_share_of_enrolled_at_da,
        RANK() OVER (PARTITION BY term ORDER BY next2_enrolled_at) AS next2_enrolled_at_rank,
        MAX(seq_no) OVER (PARTITION BY DATE_TRUNC('MONTH', second_enrolled_at_date) ORDER BY prev2_enrolled_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_seq_no_so_far,
        LEAD(seq_no, 3, 0) OVER (ORDER BY next2_enrolled_at, id) AS next3_seq_no,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', second_enrolled_at_date) ORDER BY prev2_enrolled_at_date, id) AS seq_no_2,
        ROUND((UNIX_TIMESTAMP(prev_enrolled_at) - UNIX_TIMESTAMP(LAG(prev_enrolled_at) OVER (PARTITION BY DATE_TRUNC('MONTH', second_enrolled_at_date) ORDER BY prev2_enrolled_at_date, id))) / 60, 1) AS minutes_since_prev_prev_enrolled_at,
        SIZE(COLLECT_SET(grade_status) OVER w_enrolled_at_date_dense_rank) AS distinct_grade_status_in_enrolled_at_date_dense_
    FROM with_windows
    WINDOW w_enrolled_at_date_dense_rank AS (PARTITION BY enrolled_at_date_dense_rank)
)
-- Final projection
SELECT
    id,
    term,
    enrolled_at,
    term_grade_status_label,
    student_name,
    enrolled_at_date_cume_dist,
    next2_enrolled_at,
    enrolled_at_date_percent_rank_2,
    enrolled_at_date_percent_rank_3,
    seq_no,
    prev_enrolled_at,
    prev2_enrolled_at,
    prev2_enrolled_at_date,
    days_since_prev_second_enrolled_at_date,
    max_seq_no_so_far,
    next3_seq_no,
    seq_no_2,
    minutes_since_prev_prev_enrolled_at,
    distinct_grade_status_in_enrolled_at_date_dense_,
    CONCAT(student_name, '#', CAST(next2_enrolled_at_rank AS STRING)) AS student_name_ref,
    COALESCE(next3_seq_no, 0) AS next3_seq_no_filled
FROM with_windows_2
ORDER BY next3_seq_no_filled DESC, id
LIMIT 20
