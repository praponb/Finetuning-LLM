WITH base AS (
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
    WHERE student_id <= 3
      AND term IN ('2024T2', '2025T1')
),
grouped AS (
    -- Summarise per group
    SELECT
        course_id,
        ROUND(AVG(score), 2) AS avg_score,
        ROUND(STDDEV(score), 2) AS stddev_score,
        MIN(enrolled_at) AS first_enrolled_at,
        COUNT(DISTINCT grade_status) AS distinct_grade_status,
        MAX(enrolled_at) AS last_enrolled_at,
        MIN(score) AS min_score
    FROM base
    GROUP BY course_id
    HAVING MAX(score) > 16.3
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM grouped s
)
SELECT
    course_id,
    avg_score,
    first_enrolled_at,
    distinct_grade_status,
    last_enrolled_at,
    min_score,
    enrollment_count,
    COALESCE(distinct_grade_status, 0) AS distinct_grade_status_filled
FROM profiled
