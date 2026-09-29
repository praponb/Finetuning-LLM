WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 9 AND 14
      AND student_id <= 12
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS max_enrollment_score
    FROM cleaned s
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(score_bucket) OVER (PARTITION BY student_id) AS student_id_sum_score_bucket,
        FIRST_VALUE(min_enrollment_score) IGNORE NULLS OVER (PARTITION BY term ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_min_enrollment_score,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY min_enrollment_score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at
    FROM with_counts
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(score / 25) * 25 AS score_bucket_2,
        ROUND((UNIX_TIMESTAMP(last_known_enrolled_at) - UNIX_TIMESTAMP(enrolled_at)) / 60, 1) AS minutes_enrolled_at_to_last_known_enrolled_at
    FROM with_windows
    WHERE score >= 11.3
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        term,
        score,
        enrolled_at,
        score_bucket,
        max_enrollment_score,
        student_id_sum_score_bucket,
        last_known_enrolled_at,
        score_bucket_2,
        minutes_enrolled_at_to_last_known_enrolled_at,
        LEAD(max_enrollment_score) OVER (ORDER BY last_known_enrolled_at, id) AS next_max_enrollment_score,
        RANK() OVER (ORDER BY last_known_enrolled_at, id) AS last_known_enrolled_at_rank,
        SUM(min_enrollment_score) OVER (ORDER BY last_known_enrolled_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS min_enrollment_score_sum_6_prior
    FROM tagged
),
with_parent AS (
    -- Join students
    SELECT
        s.*,
        st.is_international AS student_is_international,
        st.major AS student_major,
        st.student_name
    FROM sequenced s
    JOIN students st ON st.id = s.student_id
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_parent
    WHERE last_known_enrolled_at_rank = 1
      AND student_major <> 'CS'
)
SELECT
    id,
    student_id,
    score,
    score_bucket,
    max_enrollment_score,
    student_id_sum_score_bucket,
    last_known_enrolled_at,
    score_bucket_2,
    next_max_enrollment_score,
    last_known_enrolled_at_rank,
    min_enrollment_score_sum_6_prior,
    student_is_international,
    student_major,
    student_name,
    last_known_enrolled_at + INTERVAL 30 MINUTES AS last_known_enrolled_at_plus_30m
FROM top_rows
