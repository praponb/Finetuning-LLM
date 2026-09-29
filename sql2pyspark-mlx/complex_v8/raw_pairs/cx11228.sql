WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE (HOUR(enrolled_at) BETWEEN 9 AND 14 OR student_id <= 11)
),
grade_status_stats AS (
    -- Avg score per grade_status
    SELECT grade_status, AVG(score) AS avg_score
    FROM recent_enrollments
    GROUP BY grade_status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM recent_enrollments s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score > g.avg_score
),
enriched AS (
    -- Join students
    SELECT s.*, st.student_name
    FROM vs_group s
    LEFT JOIN students st ON st.id = s.student_id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(student_name), ' ', '_')) AS student_name_slug,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label_2
    FROM enriched
    WHERE course_id >= 3
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) FILTER (WHERE term <> '2025T1') AS enrollment_term_count
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count
    FROM scored s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
totals AS (
    -- Summarise per group
    SELECT
        grade_status,
        student_id,
        course_id,
        ROUND(SUM(avg_score), 2) AS total_avg_score,
        ROUND(AVG(days_since_enrolled_at), 2) AS avg_days_since_enrolled_at,
        ROUND(100 * COUNT(CASE WHEN grade_status = 'INCOMPLETE' THEN 1 END) / COUNT(*), 1) AS pct_grade_status_incomplete
    FROM summarized_rows
    GROUP BY grade_status, student_id, course_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        COALESCE(total_avg_score, 0.0) AS total_avg_score_filled
    FROM totals
)
-- Final projection
SELECT
    grade_status,
    student_id,
    course_id,
    avg_days_since_enrolled_at,
    grade_status_len
FROM derived
ORDER BY student_id
