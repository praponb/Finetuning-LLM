WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-13 11:00:00'
      AND student_id <= 6
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        student_id,
        term,
        ROUND(STDDEV(score), 2) AS stddev_score,
        COUNT(*) AS n_rows,
        ROUND(AVG(score), 2) AS avg_score
    FROM prepared
    GROUP BY student_id, term
    HAVING COUNT(*) >= 2
),
with_details AS (
    -- Join students
    SELECT
        s.student_id,
        s.term,
        s.stddev_score,
        s.n_rows,
        st.is_international AS student_is_international
    FROM summary s
    JOIN students st ON st.id = s.student_id
    WHERE st.is_international = TRUE
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        n_rows - stddev_score AS n_rows_minus_stddev_score,
        ABS(n_rows - 8) AS n_rows_distance,
        IF(student_is_international, 'yes', 'no') AS student_is_international_label
    FROM with_details
    WHERE student_is_international = TRUE
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM annotated
    WHERE n_rows_minus_stddev_score > (SELECT AVG(n_rows_minus_stddev_score) FROM annotated)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count,
        COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM screened s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
-- Final projection
SELECT DISTINCT stddev_score, n_rows_distance, student_id
FROM with_stats
ORDER BY stddev_score DESC
