WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek
    FROM enrollments
    WHERE (term = '2025T1' OR student_id <= 9)
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.enrolled_at_dayofweek,
        c.level AS course_level,
        c.course_code
    FROM recent_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        enrolled_at,
        enrolled_at_dayofweek,
        ROUND(CUME_DIST() OVER (PARTITION BY term ORDER BY enrolled_at_dayofweek ASC), 3) AS enrolled_at_dayofweek_cume_dist,
        LEAD(enrolled_at) OVER w_all_by_enrolled_at_dayofweek AS next_enrolled_at,
        RANK() OVER (PARTITION BY enrolled_at_dayofweek, course_level ORDER BY enrolled_at DESC, id) AS enrolled_at_rank,
        MAX(enrolled_at_dayofweek) OVER (PARTITION BY enrolled_at_dayofweek, course_level ORDER BY enrolled_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS enrolled_at_dayofweek_max_6_prior,
        ROW_NUMBER() OVER (PARTITION BY enrolled_at_dayofweek, course_level ORDER BY enrolled_at DESC, id) AS seq_no,
        enrolled_at_dayofweek - LAG(enrolled_at_dayofweek) OVER w_all_by_enrolled_at_dayofweek AS enrolled_at_dayofweek_change,
        LAG(enrolled_at) OVER w_student_id_by_enrolled_at_da AS prev_enrolled_at,
        SUM(enrolled_at_dayofweek) OVER (PARTITION BY course_id, enrolled_at_dayofweek) AS course_id_enrolled_at_dayofweek_sum_enrolled_at_,
        FIRST_VALUE(enrolled_at_dayofweek) OVER (PARTITION BY course_id ORDER BY enrolled_at_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_dayofweek,
        NTILE(10) OVER (PARTITION BY course_level, student_id ORDER BY enrolled_at, id) AS enrolled_at_decile,
        SUM(enrolled_at_dayofweek) OVER (PARTITION BY course_id ORDER BY enrolled_at_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_enrolled_at_dayofweek,
        NTH_VALUE(enrolled_at_dayofweek, 2) OVER (ORDER BY enrolled_at_dayofweek ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_dayofweek,
        LAST_VALUE(enrolled_at_dayofweek) OVER (PARTITION BY enrolled_at_dayofweek, course_level ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at_dayofweek
    FROM with_parent_course
    WINDOW w_all_by_enrolled_at_dayofweek AS (ORDER BY enrolled_at_dayofweek ASC, id), w_student_id_by_enrolled_at_da AS (PARTITION BY student_id ORDER BY enrolled_at_dayofweek NULLS LAST, id)
)
SELECT
    id,
    student_id,
    enrolled_at_dayofweek_cume_dist,
    next_enrolled_at,
    enrolled_at_rank,
    seq_no,
    enrolled_at_dayofweek_change,
    prev_enrolled_at,
    course_id_enrolled_at_dayofweek_sum_enrolled_at_,
    running_enrolled_at_dayofweek,
    last_enrolled_at_dayofweek
FROM ranked
WHERE enrolled_at_dayofweek_change <= 3
ORDER BY last_enrolled_at_dayofweek, next_enrolled_at DESC
