WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day,
        FLOOR(score / 25) * 25 AS score_bucket,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM enrollments
    WHERE student_id IS NOT NULL
),
combined_details AS (
    -- Attach student attributes
    SELECT s.*, st.student_name, st.major AS student_major
    FROM cleaned s
    JOIN students st ON st.id = s.student_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_prefix,
        student_name,
        student_major,
        ROUND(score_bucket / NULLIF(SUM(score_bucket) OVER w_student_major_course_id, 0), 4) AS score_bucket_share_of_student_major_course_id,
        COUNT(*) OVER (PARTITION BY student_major, course_id) AS rows_in_student_major_course_id,
        score_bucket - LAG(score_bucket) OVER (PARTITION BY course_id ORDER BY score_bucket DESC NULLS FIRST, id) AS score_bucket_change,
        NTILE(4) OVER w_course_id_by_score_bucket AS score_bucket_quartile,
        LAG(score, 2) OVER w_course_id_by_score_bucket AS prev2_score
    FROM combined_details
    WINDOW w_student_major_course_id AS (PARTITION BY student_major, course_id), w_course_id_by_score_bucket AS (PARTITION BY course_id ORDER BY score_bucket DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    grade_status,
    term_prefix,
    student_name,
    student_major,
    score_bucket_share_of_student_major_course_id,
    rows_in_student_major_course_id,
    score_bucket_change,
    score_bucket_quartile,
    prev2_score
FROM with_windows
ORDER BY term_prefix, student_name DESC
