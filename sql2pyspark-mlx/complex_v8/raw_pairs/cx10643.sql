WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE term = '2025T2'
),
enriched_student AS (
    -- Join students
    SELECT s.*, st.major AS student_major, st.enrolled_on AS student_enrolled_on
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        term,
        grade_status,
        enrolled_at,
        student_major,
        ROUND(CUME_DIST() OVER (PARTITION BY student_major, grade_status ORDER BY enrolled_at DESC), 3) AS enrolled_at_cume_dist,
        LEAD(enrolled_at, 3) OVER w_student_major_grade_status_b_2 AS next3_enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_major ORDER BY student_enrolled_on, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY student_major, grade_status ORDER BY enrolled_at DESC) AS enrolled_at_dense_rank,
        NTH_VALUE(student_enrolled_on, 2) OVER (PARTITION BY student_major, grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_enrolled_on,
        RANK() OVER (PARTITION BY student_major, grade_status ORDER BY enrolled_at DESC) AS enrolled_at_rank
    FROM enriched_student
    WINDOW w_student_major_grade_status_b_2 AS (PARTITION BY student_major, grade_status ORDER BY enrolled_at DESC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        term,
        enrolled_at,
        student_major,
        enrolled_at_cume_dist,
        next3_enrolled_at,
        minutes_since_prev_enrolled_at,
        enrolled_at_dense_rank,
        second_student_enrolled_on,
        enrolled_at_rank,
        NTILE(5) OVER w_grade_status_enrolled_at_ran AS second_student_enrolled_on_quintile,
        ROUND(AVG(enrolled_at_rank) OVER (PARTITION BY grade_status), 2) AS grade_status_avg_enrolled_at_rank,
        NTILE(3) OVER w_grade_status_enrolled_at_ran AS second_student_enrolled_on_tercile
    FROM windowed
    WINDOW w_grade_status_enrolled_at_ran AS (PARTITION BY grade_status, enrolled_at_rank ORDER BY second_student_enrolled_on DESC, id)
)
SELECT
    second_student_enrolled_on_quintile,
    enrolled_at_rank,
    DATEDIFF(MAX(second_student_enrolled_on), MIN(second_student_enrolled_on)) AS second_student_enrolled_on_span_days
FROM metrics
GROUP BY GROUPING SETS ((second_student_enrolled_on_quintile, enrolled_at_rank), (second_student_enrolled_on_quintile), ())
