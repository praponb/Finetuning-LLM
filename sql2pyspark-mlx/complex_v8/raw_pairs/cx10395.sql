WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        HOUR(enrolled_at) AS enrolled_at_hour,
        YEAR(enrolled_at) AS enrolled_at_year,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE course_id <= 6
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        enrolled_at_year,
        ROUND(CUME_DIST() OVER (PARTITION BY enrolled_at_year, grade_status ORDER BY score DESC), 3) AS score_cume_dist,
        LAST_VALUE(score_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_filled,
        FIRST_VALUE(score) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        ROUND(enrolled_at_year / NULLIF(SUM(enrolled_at_year) OVER (PARTITION BY term, student_id), 0), 4) AS enrolled_at_year_share_of_term_student_id,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY enrolled_at, id) AS seq_no
    FROM filtered_enrollments
),
with_activity AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.gpa < 3.2
        )
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    seq_no,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT student_id) AS distinct_student_ids,
    MIN(last_score_filled) AS min_last_score_filled
FROM with_activity
GROUP BY GROUPING SETS ((grade_status, seq_no), (grade_status), (seq_no), ())
ORDER BY grade_status NULLS LAST, seq_no NULLS LAST
