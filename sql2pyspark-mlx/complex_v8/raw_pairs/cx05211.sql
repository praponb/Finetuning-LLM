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
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE grade_status <> 'FAIL'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        ROUND(score / NULLIF(score_filled, 0), 3) AS score_per_score_filled,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM valid
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_filled,
        s.enrolled_at_label,
        s.score_per_score_filled,
        s.term_prefix,
        c.level AS course_level
    FROM labeled s
    JOIN courses c ON c.id = s.course_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    COALESCE(course_level, 'ALL') AS course_level,
    GROUPING(grade_status) AS is_total_grade_status,
    COUNT(*) AS cnt
FROM with_parent
GROUP BY GROUPING SETS ((grade_status, course_level), (grade_status), (course_level))
