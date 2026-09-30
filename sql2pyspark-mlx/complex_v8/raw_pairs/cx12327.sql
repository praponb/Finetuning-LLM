WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 55.6) AS score_distance,
        LOWER(TRIM(term)) AS term_clean,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
    WHERE grade_status = 'PASS'
      AND student_id >= 19
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at
    FROM base s
)
-- Subtotals with GROUPING SETS
SELECT
    grade_status,
    student_id,
    term,
    GROUPING(grade_status) AS is_total_grade_status,
    ROUND(SUM(score_adj) / NULLIF(SUM(score_distance), 0), 3) AS score_adj_to_score_distance_ratio
FROM with_counts
GROUP BY GROUPING SETS ((grade_status, student_id, term), (grade_status), (student_id))
ORDER BY grade_status NULLS LAST, student_id NULLS LAST, term NULLS LAST
