WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        QUARTER(enrolled_at) AS enrolled_at_quarter,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE grade_status IN ('WITHDRAWN', 'FAIL', 'INCOMPLETE')
      AND score < 78.8
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        enrolled_at_quarter + score AS enrolled_at_quarter_plus_score,
        ABS(days_since_enrolled_at - 385) AS days_since_enrolled_at_distance
    FROM filtered_enrollments
),
term_stats AS (
    -- Avg enrolled_at_quarter_plus_score per term
    SELECT term, AVG(enrolled_at_quarter_plus_score) AS avg_enrolled_at_quarter_plus_score
    FROM enriched
    GROUP BY term
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_enrolled_at_quarter_plus_score,
        ROUND(s.enrolled_at_quarter_plus_score - g.avg_enrolled_at_quarter_plus_score, 2) AS enrolled_at_quarter_plus_score_minus_avg
    FROM enriched s
    JOIN term_stats g ON g.term = s.term
    WHERE s.enrolled_at_quarter_plus_score > g.avg_enrolled_at_quarter_plus_score
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.enrolled_at_year,
        s.term_slug,
        s.days_since_enrolled_at_distance,
        s.avg_enrolled_at_quarter_plus_score,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status
    FROM compared s
    JOIN enrollments e ON e.student_id = s.student_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(term, 'ALL') AS term,
    course_id,
    enrollment_course_id,
    GROUPING_ID() AS grouping_level,
    ROUND(AVG(enrollment_score), 2) AS avg_enrollment_score,
    ROUND(100 * COUNT(CASE WHEN term NOT IN ('2024T2', '2025T2') THEN 1 END) / COUNT(*), 1) AS pct_term_not_in_2024t2_2025t2,
    MIN(enrolled_at) AS first_enrolled_at
FROM line_level
GROUP BY GROUPING SETS ((term, course_id, enrollment_course_id), (term))
ORDER BY term NULLS LAST, course_id NULLS LAST, enrollment_course_id NULLS LAST
