WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at,
        INITCAP(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE score < 80.2
),
joined AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        c.course_code,
        st.enrolled_on AS student_enrolled_on,
        st.major AS student_major
    FROM valid s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
    WHERE UPPER(TRIM(c.course_code)) LIKE 'C%'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        student_enrolled_on,
        student_major,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id, YEAR(student_enrolled_on) ORDER BY student_enrolled_on ASC), 3) AS student_enrolled_on_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, YEAR(student_enrolled_on) ORDER BY student_enrolled_on ASC, id), 3) AS student_enrolled_on_percent_rank,
        LEAD(enrolled_at) OVER (ORDER BY enrolled_at ASC, id) AS next_enrolled_at,
        ROUND(PERCENT_RANK() OVER w_student_id_student_enrolled, 3) AS student_enrolled_on_percent_rank_2,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY student_enrolled_on, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        LAST_VALUE(student_enrolled_on) OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_student_enrolled_on,
        RANK() OVER (ORDER BY enrolled_at ASC) AS enrolled_at_rank
    FROM joined
    WINDOW w_student_id_student_enrolled AS (PARTITION BY student_id, YEAR(student_enrolled_on) ORDER BY student_enrolled_on ASC)
),
top_rows AS (
    -- Top 2 per group
    SELECT *
    FROM windowed
    WHERE enrolled_at_rank <= 2
      AND student_enrolled_on_cume_dist < 0.7
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(enrolled_at_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', last_student_enrolled_on) ORDER BY student_enrolled_on_percent_rank_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_enrolled_at_rank_so_far,
        ROUND(enrolled_at_rank - AVG(enrolled_at_rank) OVER w_student_enrolled_on_month, 2) AS enrolled_at_rank_vs_student_enrolled_on_month_av,
        ROUND(enrolled_at_rank / NULLIF(SUM(enrolled_at_rank) OVER (PARTITION BY student_id, course_id), 0), 4) AS enrolled_at_rank_share_of_student_id_course_id,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY next_enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at
    FROM top_rows
    WINDOW w_student_enrolled_on_month AS (PARTITION BY DATE_TRUNC('MONTH', student_enrolled_on))
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    student_enrolled_on,
    student_enrolled_on_cume_dist,
    student_enrolled_on_percent_rank,
    next_enrolled_at,
    student_enrolled_on_percent_rank_2,
    minutes_since_prev_enrolled_at,
    enrolled_at_rank,
    max_enrolled_at_rank_so_far,
    enrolled_at_rank_vs_student_enrolled_on_month_av,
    enrolled_at_rank_share_of_student_id_course_id,
    first_known_enrolled_at,
    COALESCE(term, 'UNKNOWN') AS term_or_unknown
FROM metrics
ORDER BY student_id DESC
