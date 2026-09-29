WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE grade_status IN ('WITHDRAWN', 'PASS', 'INCOMPLETE')
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM scoped
    WHERE score > (SELECT AVG(score) FROM scoped)
)
-- Subtotals with GROUPING SETS
SELECT
    grade_status,
    term_len,
    ROUND(SUM(score) / NULLIF(SUM(term_len), 0), 3) AS score_to_term_len_ratio
FROM with_activity
GROUP BY GROUPING SETS ((grade_status, term_len), (grade_status), (term_len))
ORDER BY grade_status NULLS LAST, term_len NULLS LAST
