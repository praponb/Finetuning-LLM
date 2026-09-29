WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE student_id <= 8
),
course_id_stats AS (
    -- Median score per course_id
    SELECT course_id, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM prepared
    GROUP BY course_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM prepared s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score >= g.median_score
),
student_id_stats AS (
    -- Max score per student_id
    SELECT student_id, MAX(score) AS max_score
    FROM outliers
    GROUP BY student_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_score, ROUND(s.score - g.max_score, 2) AS score_minus_max
    FROM outliers s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score = g.max_score
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.median_score,
        s.max_score,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM above_average s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
)
SELECT
    id,
    student_id,
    score,
    median_score,
    enrollment_id,
    enrollment_course_id,
    FLOOR(max_score / 25) * 25 AS max_score_bucket
FROM line_level
ORDER BY max_score_bucket
