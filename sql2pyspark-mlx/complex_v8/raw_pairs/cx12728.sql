WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len,
        LENGTH(TRIM(term)) AS term_len_2,
        LENGTH(TRIM(term)) AS term_len_3
    FROM enrollments
    WHERE student_id <= 17
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.term_len,
        s.term_len_2,
        s.term_len_3,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.student_id AS enrollment_student_id
    FROM recent s
    JOIN enrollments e ON e.course_id = s.course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE e.term = '2025T2') FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_term_count,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at
    FROM exploded_detail s
)
SELECT
    term_len,
    term,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt
FROM summarized_rows
GROUP BY CUBE(term_len, term)
