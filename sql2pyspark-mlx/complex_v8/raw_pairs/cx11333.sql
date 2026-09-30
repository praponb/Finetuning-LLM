WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE term = '2025T1'
      AND grade_status IS NOT NULL
),
without_matches AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.dept NOT IN ('Biz', 'Math')
        )
),
grade_status_stats AS (
    -- Max score per grade_status
    SELECT grade_status, MAX(score) AS max_score
    FROM without_matches
    GROUP BY grade_status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_score, ROUND(s.score - g.max_score, 2) AS score_minus_max
    FROM without_matches s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score = g.max_score
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM above_average s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term = '2025T2'
              AND e.student_id IS NOT NULL
        )
)
SELECT
    id,
    student_id,
    term,
    score,
    grade_status,
    grade_status_or_unknown,
    max_score,
    score_minus_max,
    LEAST(score, max_score) AS least_score_max_score
FROM qualified
ORDER BY id, term DESC
