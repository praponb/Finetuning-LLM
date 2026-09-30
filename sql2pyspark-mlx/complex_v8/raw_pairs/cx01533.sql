WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE student_id <= 18
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id
    FROM prepared s
    JOIN enrollments e ON e.student_id = s.student_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        enrollment_id,
        enrollment_term,
        ROW_NUMBER() OVER (ORDER BY enrolled_at DESC NULLS FIRST, id, enrollment_id) AS seq_no,
        SUM(enrollment_score) OVER w_enrollment_id AS enrollment_id_sum_enrollment_score
    FROM exploded_detail_enrollments
    WINDOW w_enrollment_id AS (PARTITION BY enrollment_id)
),
joined_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrollment_id,
        s.enrollment_term,
        s.seq_no,
        c.level AS course_level
    FROM with_windows s
    JOIN courses c ON c.id = s.course_id
    WHERE c.level NOT IN ('advanced', 'core')
)
SELECT
    course_id,
    COALESCE(enrollment_term, 'ALL') AS enrollment_term,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(enrollment_term)) AS enrollment_term_list,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    MAX(seq_no) AS max_seq_no
FROM joined_course
GROUP BY GROUPING SETS ((course_id, enrollment_term), (course_id), (enrollment_term), ())
ORDER BY course_id NULLS LAST, enrollment_term NULLS LAST
