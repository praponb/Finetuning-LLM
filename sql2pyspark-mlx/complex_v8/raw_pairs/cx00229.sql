WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE course_id <= 5
),
with_prev AS (
    -- Previous enrolled_at per term_or_unknown
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term_or_unknown ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY term_or_unknown ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY prev_enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(burst_id - AVG(burst_id) OVER (PARTITION BY student_id), 2) AS burst_id_vs_student_id_avg,
        ROUND(AVG(burst_id) OVER (ORDER BY prev_enrolled_at ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS burst_id_avg_6_prior
    FROM sessionized
)
-- Subtotals with ROLLUP
SELECT
    course_id,
    COALESCE(term_or_unknown, 'ALL') AS term_or_unknown,
    SUM(rows_in_student_id) AS total_rows_in_student_id,
    COUNT(*) AS row_count,
    SUM(CASE WHEN grade_status = 'FAIL' THEN score ELSE 0 END) AS score_grade_status_fail
FROM with_windows
GROUP BY ROLLUP(course_id, term_or_unknown)
