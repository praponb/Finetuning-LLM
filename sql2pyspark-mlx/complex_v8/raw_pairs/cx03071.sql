WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE term NOT IN ('2025T2', '2024T1')
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM prepared_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term <> '2025T2'
              AND e.grade_status = 'INCOMPLETE'
        )
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at
    FROM without_matches s
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        term,
        grade_status,
        score_bucket,
        ROUND(score_bucket / NULLIF(SUM(score_bucket) OVER w_term_last_enrollment_enrolle, 0), 4) AS score_bucket_share_of_term_last_enrollment_enrol,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY term, YEAR(last_enrollment_enrolled_at))) AS distinct_student_id_in_term_last_enrollment_enro,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(enrolled_at), grade_status ORDER BY score_bucket DESC, id), 3) AS score_bucket_cume_dist
    FROM with_stats
    WINDOW w_term_last_enrollment_enrolle AS (PARTITION BY term, YEAR(last_enrollment_enrolled_at))
),
with_details_course AS (
    -- Attach course attributes
    SELECT s.*, c.course_code, c.credits AS course_credits
    FROM with_trends s
    JOIN courses c ON c.id = s.course_id
),
with_parent_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.course_id,
        s.score_bucket,
        s.distinct_student_id_in_term_last_enrollment_enro,
        s.course_code,
        s.course_credits,
        c.credits AS course_credits_2,
        c.level AS course_level
    FROM with_details_course s
    JOIN courses c ON c.id = s.course_id AND c.dept IN ('Biz', 'Math', 'Bio')
    WHERE c.level <> 'core'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        distinct_student_id_in_term_last_enrollment_enro,
        course_code,
        course_credits,
        course_credits_2,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        LAG(course_credits, 2) OVER (ORDER BY score_bucket, id) AS prev2_course_credits
    FROM with_parent_course
)
-- Final projection
SELECT
    id,
    course_id,
    distinct_student_id_in_term_last_enrollment_enro,
    course_code,
    course_credits,
    rows_in_course_id,
    prev2_course_credits,
    LENGTH(TRIM(course_code)) AS course_code_len
FROM with_windows
WHERE course_credits <= 3
ORDER BY id, course_code_len DESC
