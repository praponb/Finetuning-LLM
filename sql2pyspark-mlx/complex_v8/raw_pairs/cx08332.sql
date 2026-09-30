WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2025T2', '2025T1') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 10 AND 12
      AND grade_status IN ('WITHDRAWN', 'INCOMPLETE')
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix,
        COALESCE(score, 0.0) AS score_filled
    FROM valid_enrollments
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at_date ASC, id), 3) AS enrolled_at_date_percent_rank,
        MAX(score_filled) OVER (ORDER BY enrolled_at_date ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_filled_max_1_prior
    FROM annotated
),
combined_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.term_group,
        s.term_slug,
        s.grade_status_prefix,
        s.score_filled,
        s.enrolled_at_date_percent_rank,
        s.score_filled_max_1_prior,
        c.credits AS course_credits,
        c.dept AS course_dept
    FROM sequenced s
    JOIN courses c ON c.id = s.course_id
),
without_matches AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM combined_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.enrolled_on IS NOT NULL
              AND st.gpa > 2.1
        )
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score <= 79.3
              AND e.term = '2024T2'
        )
),
course_id_stats AS (
    -- Avg score_filled per course_id
    SELECT course_id, AVG(score_filled) AS avg_score_filled
    FROM filtered
    GROUP BY course_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_score_filled,
        ROUND(s.score_filled - g.avg_score_filled, 2) AS score_filled_minus_avg
    FROM filtered s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score_filled > g.avg_score_filled
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    term_group,
    term_slug,
    grade_status_prefix,
    score_filled,
    enrolled_at_date_percent_rank,
    score_filled_max_1_prior,
    course_credits,
    score_filled_minus_avg
FROM above_average
ORDER BY grade_status
