WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE term = '2025T1'
),
grade_status_stats AS (
    -- Avg score per grade_status
    SELECT grade_status, AVG(score) AS avg_score
    FROM valid_enrollments
    GROUP BY grade_status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM valid_enrollments s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score > g.avg_score
),
joined AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.days_since_enrolled_at,
        s.avg_score,
        s.score_minus_avg,
        st.gpa AS student_gpa
    FROM vs_group s
    JOIN students st ON st.id = s.student_id
    WHERE st.gpa >= 2.7
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM joined
    WHERE score_minus_avg > (SELECT AVG(score_minus_avg) FROM joined)
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM without_matches s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        days_since_enrolled_at,
        score_minus_avg,
        student_gpa,
        GREATEST(days_since_enrolled_at, enrollment_count) AS greatest_days_since_enrolled_at_enrollment_count
    FROM summarized_rows
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.days_since_enrolled_at,
        s.student_gpa,
        s.greatest_days_since_enrolled_at_enrollment_count,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM enriched s
    JOIN enrollments e ON e.course_id = s.course_id AND e.enrolled_at >= TIMESTAMP '2025-01-19 15:00:00'
)
-- Final projection
SELECT
    id,
    course_id,
    score,
    enrolled_at,
    days_since_enrolled_at,
    student_gpa,
    greatest_days_since_enrolled_at_enrollment_count,
    enrollment_student_id,
    enrollment_score,
    ROUND(student_gpa / NULLIF(score, 0), 3) AS student_gpa_per_score,
    SUBSTRING(TRIM(enrollment_grade_status), 1, 2) AS enrollment_grade_status_prefix
FROM exploded_detail_enrollments
ORDER BY score DESC
