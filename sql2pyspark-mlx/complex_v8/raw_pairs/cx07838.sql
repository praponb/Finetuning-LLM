WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-03-08 11:00:00'
),
per_group AS (
    -- Summarise per group
    SELECT
        course_id,
        grade_status,
        term,
        MIN(score) AS min_score,
        MIN(enrolled_at) AS first_enrolled_at,
        PERCENTILE_APPROX(score, 0.5) AS median_score,
        MAX(enrolled_at) AS last_enrolled_at,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list
    FROM valid
    WHERE score <= 57.3
    GROUP BY 1, 2, 3
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM per_group s
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        ROUND(AVG(score), 2) AS avg_enrollment_score,
        ROUND(AVG(score), 2) AS avg_enrollment_score_2,
        SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.avg_enrollment_score,
        e.avg_enrollment_score_2,
        e.total_enrollment_score
    FROM with_counts s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.course_id
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.course_id,
        s.grade_status,
        s.term,
        s.min_score,
        s.avg_enrollment_score,
        s.total_enrollment_score,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status
    FROM summarized_rows s
    LEFT JOIN enrollments e ON e.course_id = s.course_id AND e.enrolled_at < TIMESTAMP '2025-03-02 09:00:00'
)
SELECT
    course_id,
    term,
    min_score,
    avg_enrollment_score,
    total_enrollment_score,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_enrolled_at + INTERVAL 30 MINUTES AS enrollment_enrolled_at_plus_30m
FROM line_level
WHERE TO_DATE(enrollment_enrolled_at) < DATE '2025-02-18'
ORDER BY course_id DESC, term DESC
