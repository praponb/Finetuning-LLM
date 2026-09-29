WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY YEAR(enrolled_at)), 0), 4) AS score_share_of_enrolled_at_year,
        DENSE_RANK() OVER (PARTITION BY session_no ORDER BY score ASC, id) AS score_dense_rank
    FROM sessionized
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score <= 79.2
              AND e.term IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    term_len,
    prev_enrolled_at,
    starts_session,
    session_no,
    score_share_of_enrolled_at_year,
    score_dense_rank,
    CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref
FROM without_matches
ORDER BY term
