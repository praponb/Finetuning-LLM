WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE term IN ('2024T1', '2025T2', '2025T1')
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM recent_enrollments s
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY term_len, student_id ORDER BY term_len ASC, id) AS term_len_quartile,
        ROUND(PERCENT_RANK() OVER w_term_len_student_id_by_term, 3) AS term_len_percent_rank,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY grade_status)) AS distinct_student_id_in_grade_status
    FROM with_counts
    WINDOW w_term_len_student_id_by_term AS (PARTITION BY term_len, student_id ORDER BY term_len ASC, id)
)
SELECT
    id,
    score,
    grade_status,
    term_len,
    avg_enrollment_score,
    enrollment_count,
    term_len_quartile,
    term_len_percent_rank,
    distinct_student_id_in_grade_status
FROM with_trends
ORDER BY grade_status NULLS LAST, term_len_percent_rank DESC
