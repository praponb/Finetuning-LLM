WITH cleaned AS (
    -- Keep valid rows and derive helper columns
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
    WHERE term = '2024T1'
      AND student_id IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY student_id ORDER BY enrolled_at_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(score) OVER (PARTITION BY student_id ORDER BY enrolled_at_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score
    FROM cleaned
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at_dayofweek,
        s.running_count,
        s.running_score,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM sequenced s
    JOIN enrollments e ON e.course_id = s.course_id
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at
    FROM with_children s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
summarized_rows_2 AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score
    FROM summarized_rows s
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    running_count,
    running_score,
    enrollment_id,
    enrollment_grade_status,
    enrollment_score,
    avg_enrollment_score
FROM summarized_rows_2
ORDER BY enrollment_grade_status DESC, id, enrollment_id
LIMIT 5
