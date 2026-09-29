WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status = 'INCOMPLETE'
),
filtered AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.credits >= 2
              AND c.course_code IS NOT NULL
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC), 3) AS enrolled_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_grade_status_by_enrolled_at_4)) / 60, 1) AS minutes_since_prev_enrolled_at,
        DENSE_RANK() OVER w_grade_status_by_enrolled_at_3 AS enrolled_at_dense_rank,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at
    FROM filtered
    WINDOW w_grade_status_by_enrolled_at_4 AS (PARTITION BY grade_status ORDER BY enrolled_at DESC, id), w_grade_status_by_enrolled_at_3 AS (PARTITION BY grade_status ORDER BY enrolled_at DESC)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM windowed
    WHERE enrolled_at_dense_rank <= 2
),
combined_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.first_known_enrolled_at,
        s.last_enrolled_at,
        s.minutes_since_prev_enrolled_at,
        st.gpa AS student_gpa,
        st.enrolled_on AS student_enrolled_on,
        st.major AS student_major
    FROM leaders s
    LEFT JOIN students st ON st.id = s.student_id
),
summary AS (
    -- Summarise per group
    SELECT
        TO_DATE(first_known_enrolled_at) AS first_known_enrolled_at_day,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        ROUND(AVG(minutes_since_prev_enrolled_at), 2) AS avg_minutes_since_prev_enrolled_at,
        MIN(minutes_since_prev_enrolled_at) AS min_minutes_since_prev_enrolled_at,
        SUM(minutes_since_prev_enrolled_at) FILTER (WHERE term = '2025T1') AS minutes_since_prev_enrolled_at_term_2025t1,
        MIN(student_enrolled_on) AS first_student_enrolled_on,
        ROUND(SUM(student_gpa), 2) AS total_student_gpa
    FROM combined_details
    WHERE student_gpa >= 2.7
    GROUP BY TO_DATE(first_known_enrolled_at)
)
SELECT
    first_known_enrolled_at_day,
    distinct_course_ids,
    avg_minutes_since_prev_enrolled_at,
    minutes_since_prev_enrolled_at_term_2025t1,
    first_student_enrolled_on,
    distinct_course_ids + total_student_gpa AS distinct_course_ids_plus_total_student_gpa
FROM summary
ORDER BY first_student_enrolled_on
