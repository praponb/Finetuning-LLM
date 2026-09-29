WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.prev_enrolled_at,
        s.starts_session,
        c.course_code,
        c.credits AS course_credits,
        c.level AS course_level
    FROM sessionized s
    LEFT JOIN courses c ON c.id = s.course_id
)
SELECT
    id,
    course_id,
    enrolled_at,
    prev_enrolled_at,
    starts_session,
    course_code,
    course_level
FROM combined_details
ORDER BY id
LIMIT 10
