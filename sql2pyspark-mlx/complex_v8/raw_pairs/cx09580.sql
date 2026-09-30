WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        INITCAP(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE score <= 78.8
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, ROUND(AVG(score), 2) AS avg_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.avg_enrollment_score
    FROM valid s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    grade_status_clean,
    avg_enrollment_score,
    LOWER(TRIM(grade_status_clean)) AS grade_status_clean_clean,
    CASE WHEN score >= 69.6 THEN 'A' WHEN score >= 23.7 THEN 'B' ELSE 'C' END AS score_tier
FROM profiled
ORDER BY id DESC, grade_status_clean
