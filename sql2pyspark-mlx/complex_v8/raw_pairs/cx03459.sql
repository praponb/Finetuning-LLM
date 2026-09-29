WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE score >= 10.8
),
grade_status_stats AS (
    -- Max score per grade_status
    SELECT grade_status, MAX(score) AS max_score
    FROM prepared
    GROUP BY grade_status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_score, ROUND(s.score - g.max_score, 2) AS score_minus_max
    FROM prepared s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score = g.max_score
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM above_average s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
      AND s.grade_status IN ('PASS', 'INCOMPLETE')
),
labeled AS (
    -- Label and score rows
    SELECT *, ROUND(score_minus_max * 0.9, 2) AS score_minus_max_adj
    FROM qualified
    WHERE term_ref IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        grade_status,
        max_score,
        score_minus_max,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), term ORDER BY score DESC, id) AS score_seq,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), term ORDER BY score DESC, id) AS score_rank
    FROM labeled
)
SELECT
    id,
    student_id,
    term,
    score_seq,
    ROUND(max_score / NULLIF(score_rank, 0), 3) AS max_score_per_score_rank,
    ROUND(max_score / NULLIF(score_rank, 0), 3) AS max_score_per_score_rank_2
FROM with_windows
WHERE student_id IS NOT NULL
ORDER BY max_score_per_score_rank_2
