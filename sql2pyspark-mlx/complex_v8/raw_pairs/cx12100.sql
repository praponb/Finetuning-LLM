WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 34.2) AS score_distance
    FROM enrollments
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY course_id, grade_status ORDER BY score_distance DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY course_id, grade_status ORDER BY score_distance DESC, id) AS score_distance_dense_rank,
        LEAD(score) OVER (PARTITION BY course_id, term ORDER BY score DESC, id) AS next_score,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status
    FROM prepared
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        score,
        grade_status,
        enrolled_at,
        score_distance,
        score_distance_dense_rank,
        next_score,
        rows_in_grade_status,
        LEAD(enrolled_at, 3) OVER (PARTITION BY score_distance_dense_rank, student_id ORDER BY rows_in_grade_status, id) AS next3_enrolled_at,
        MAX(score_distance_dense_rank) OVER (ORDER BY next_score ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_distance_dense_rank_max_6_prior,
        COUNT(*) OVER w_course_id AS rows_in_course_id,
        ROW_NUMBER() OVER (PARTITION BY score_distance_dense_rank, student_id ORDER BY rows_in_grade_status, id) AS rn,
        SUM(next_score) OVER (PARTITION BY score_distance_dense_rank, student_id ORDER BY rows_in_grade_status, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_score,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_id)) AS distinct_grade_status_in_course_id
    FROM with_windows
    WINDOW w_course_id AS (PARTITION BY course_id)
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.term IN ('2025T1', '2025T2') THEN 1 END) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_term_count
    FROM windowed s
)
SELECT
    id,
    student_id,
    grade_status,
    enrolled_at,
    score_distance_dense_rank,
    next3_enrolled_at,
    rows_in_course_id,
    rn,
    running_next_score,
    distinct_grade_status_in_course_id,
    COALESCE(next_score, 0.0) AS next_score_filled
FROM profiled
