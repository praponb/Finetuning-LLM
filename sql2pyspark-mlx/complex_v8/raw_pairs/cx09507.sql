WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE term IS NOT NULL
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count,
        SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.total_enrollment_score
    FROM base_enrollments s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(grade_status_len) OVER (PARTITION BY course_id ORDER BY enrollment_count DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS grade_status_len_avg_3,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY enrollment_count)) AS distinct_term_in_enrollment_count,
        ROUND(enrollment_count / NULLIF(SUM(enrollment_count) OVER (PARTITION BY enrollment_count), 0), 4) AS enrollment_count_share_of_enrollment_count,
        NTH_VALUE(enrollment_count, 2) OVER (PARTITION BY course_id ORDER BY enrollment_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrollment_count,
        MAX(enrollment_count) OVER (PARTITION BY TO_DATE(last_enrollment_enrolled_at), grade_status_len ORDER BY score DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS enrollment_count_max_2_prior,
        ROW_NUMBER() OVER w_all_by_score_filled AS seq_no
    FROM with_stats
    WINDOW w_all_by_score_filled AS (ORDER BY score_filled, id)
),
with_parent_student AS (
    -- Attach student attributes
    SELECT s.*, st.is_international AS student_is_international
    FROM windowed s
    LEFT JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    grade_status_len,
    last_enrollment_enrolled_at,
    enrollment_count,
    total_enrollment_score,
    grade_status_len_avg_3,
    enrollment_count_share_of_enrollment_count,
    second_enrollment_count,
    seq_no,
    student_is_international
FROM with_parent_student
WHERE term IS NOT NULL
ORDER BY enrolled_at, student_id
