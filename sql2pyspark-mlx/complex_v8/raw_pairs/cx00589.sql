WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        INITCAP(TRIM(grade_status)) AS grade_status_clean,
        CASE WHEN score >= 57.6 THEN 'large' WHEN score >= 20.2 THEN 'medium' ELSE 'small' END AS score_tier,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM enrollments
    WHERE (course_id IS NOT NULL OR score >= 20.2)
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status_clean,
        s.score_tier,
        s.enrolled_at_label,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term
    FROM filtered s
    JOIN enrollments e ON e.course_id = s.course_id
),
score_tier_stats AS (
    -- Avg score per score_tier
    SELECT score_tier, AVG(score) AS avg_score
    FROM detail_rows_enrollments
    GROUP BY score_tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM detail_rows_enrollments s
    JOIN score_tier_stats g ON g.score_tier = s.score_tier
    WHERE s.score > g.avg_score
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        score,
        grade_status_clean,
        enrollment_id,
        avg_score,
        score_minus_avg,
        GREATEST(score_minus_avg, score) AS greatest_score_minus_avg_score
    FROM vs_group
    WHERE enrollment_term <> '2024T2'
)
SELECT
    id,
    score,
    grade_status_clean,
    enrollment_id,
    score_minus_avg,
    greatest_score_minus_avg_score,
    LENGTH(TRIM(grade_status_clean)) AS grade_status_clean_len
FROM tagged
WHERE student_id >= 2
ORDER BY enrollment_id
