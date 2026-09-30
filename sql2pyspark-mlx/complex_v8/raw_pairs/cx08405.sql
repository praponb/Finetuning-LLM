WITH prepared_enrollments AS (
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
    WHERE student_id IS NOT NULL
),
combined_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        st.major AS student_major,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international
    FROM prepared_enrollments s
    JOIN students st ON st.id = s.student_id AND st.major = 'CS'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY student_is_international ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        LEAD(enrolled_at) OVER w_all_by_score AS next_enrolled_at,
        LAST_VALUE(score) IGNORE NULLS OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        LAG(score) OVER (PARTITION BY student_major, student_id ORDER BY enrolled_at, id) AS prev_score,
        LEAD(score) OVER w_student_major_student_id_by AS next_score,
        ROW_NUMBER() OVER (PARTITION BY student_is_international, student_major ORDER BY enrolled_at, id) AS enrolled_at_seq,
        FIRST_VALUE(score) OVER (PARTITION BY student_is_international ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY term, grade_status)) AS distinct_course_id_in_term_grade_status,
        MAX(score) OVER (PARTITION BY student_is_international, student_major ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far
    FROM combined_details
    WINDOW w_all_by_score AS (ORDER BY score DESC, id), w_student_major_student_id_by AS (PARTITION BY student_major, student_id ORDER BY enrolled_at, id)
)
SELECT
    id,
    student_id,
    score,
    grade_status,
    enrolled_at,
    student_major,
    student_cohort,
    second_enrolled_at,
    next_enrolled_at,
    prev_score,
    next_score,
    first_score,
    distinct_course_id_in_term_grade_status,
    max_score_so_far,
    ROUND((UNIX_TIMESTAMP(next_enrolled_at) - UNIX_TIMESTAMP(second_enrolled_at)) / 3600, 1) AS hours_second_enrolled_at_to_next_enrolled_at,
    TO_DATE(next_enrolled_at) AS next_enrolled_at_date
FROM windowed
WHERE first_known_score <= 71.0
ORDER BY student_id, score NULLS LAST, id
LIMIT 10
