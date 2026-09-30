WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE grade_status <> 'WITHDRAWN'
      AND score <= 57.6
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS min_enrollment_score,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at,
        (SELECT COUNT(CASE WHEN e.term = '2024T1' THEN 1 END) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_term_count
    FROM base_enrollments s
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_all_by_enrollment_term_count AS enrollment_term_count_rank,
        ROUND(grade_status_len - AVG(grade_status_len) OVER (PARTITION BY TO_DATE(enrolled_at), term), 2) AS grade_status_len_vs_enrolled_at_to_date_term_avg
    FROM with_stats
    WINDOW w_all_by_enrollment_term_count AS (ORDER BY enrollment_term_count)
),
deduped AS (
    -- Keep the latest row per term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        grade_status_len,
        min_enrollment_score,
        last_enrollment_enrolled_at,
        enrollment_term_count,
        enrollment_term_count_rank,
        grade_status_len_vs_enrolled_at_to_date_term_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term ORDER BY enrollment_term_count DESC, id DESC) AS recency_rank
        FROM ranked
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status_len,
    min_enrollment_score,
    last_enrollment_enrolled_at,
    enrollment_term_count,
    grade_status_len_vs_enrolled_at_to_date_term_avg
FROM deduped
WHERE course_id <= 2
ORDER BY id DESC, grade_status_len_vs_enrolled_at_to_date_term_avg DESC
