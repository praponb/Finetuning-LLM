WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 10 AND 14
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        enrolled_at,
        grade_status_prefix,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY grade_status, term ORDER BY enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        LAG(grade_status_len, 2) OVER (PARTITION BY student_id, term ORDER BY score DESC NULLS FIRST, id) AS prev2_grade_status_len,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status, term ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist,
        ROUND(grade_status_len / NULLIF(SUM(grade_status_len) OVER (PARTITION BY student_id), 0), 4) AS grade_status_len_share_of_student_id,
        LAG(score, 2) OVER (PARTITION BY grade_status, term ORDER BY enrolled_at, id) AS prev2_score,
        LEAD(enrolled_at) OVER (PARTITION BY grade_status, term ORDER BY enrolled_at, id) AS next_enrolled_at
    FROM recent_enrollments
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status NOT IN ('INCOMPLETE', 'WITHDRAWN')
        )
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        MAX(enrolled_at) AS last_enrollment_enrolled_at_2
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at,
        e.last_enrollment_enrolled_at,
        e.last_enrollment_enrolled_at_2
    FROM without_matches s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY score), 3) AS score_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_percent_rank,
        COUNT(*) OVER (PARTITION BY student_id, grade_status) AS rows_in_student_id_grade_status,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_cume_dist_2,
        DENSE_RANK() OVER (ORDER BY score) AS score_dense_rank
    FROM with_counts
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    score,
    grade_status,
    enrolled_at,
    grade_status_prefix,
    minutes_since_prev_enrolled_at,
    prev2_grade_status_len,
    grade_status_len_share_of_student_id,
    prev2_score,
    first_enrollment_enrolled_at,
    last_enrollment_enrolled_at,
    last_enrollment_enrolled_at_2,
    score_percent_rank,
    enrolled_at_percent_rank,
    rows_in_student_id_grade_status,
    enrolled_at_cume_dist_2,
    score_dense_rank,
    ABS(score - 42.4) AS score_distance
FROM sequenced
WHERE enrolled_at < TIMESTAMP '2025-02-24 13:00:00'
