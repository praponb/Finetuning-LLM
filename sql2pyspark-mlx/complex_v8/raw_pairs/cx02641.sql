WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE score >= 38.3
      AND term = '2025T1'
),
term_stats AS (
    -- Avg score per term
    SELECT term, AVG(score) AS avg_score
    FROM filtered
    GROUP BY term
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM filtered s
    JOIN term_stats g ON g.term = s.term
    WHERE s.score > g.avg_score
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM compared s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    term_ref,
    enrolled_at_label,
    avg_score,
    score_minus_avg,
    enrollment_count,
    ABS(score_minus_avg - -10.0) AS score_minus_avg_distance,
    score_minus_avg + avg_score AS score_minus_avg_plus_avg_score
FROM with_counts
