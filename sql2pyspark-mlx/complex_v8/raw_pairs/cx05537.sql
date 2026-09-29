WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        enrolled_at + INTERVAL 1 DAYS AS enrolled_at_plus_1d
    FROM enrollments
    WHERE course_id >= 9
      AND term = '2025T2'
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at,
        (SELECT COUNT(CASE WHEN e.term IS NOT NULL THEN 1 END) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_term_count
    FROM base s
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at_date,
        enrolled_at_plus_1d,
        last_enrollment_enrolled_at,
        enrollment_term_count,
        NTILE(5) OVER (PARTITION BY MONTH(enrolled_at), course_id ORDER BY last_enrollment_enrolled_at ASC NULLS LAST, id) AS last_enrollment_enrolled_at_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(enrolled_at), course_id ORDER BY last_enrollment_enrolled_at ASC NULLS LAST), 3) AS last_enrollment_enrolled_at_cume_dist,
        LAST_VALUE(enrolled_at_date) IGNORE NULLS OVER (PARTITION BY MONTH(enrolled_at), course_id ORDER BY last_enrollment_enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at_date,
        FIRST_VALUE(enrolled_at_plus_1d) IGNORE NULLS OVER (PARTITION BY MONTH(enrolled_at), course_id ORDER BY last_enrollment_enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at_plus_1d
    FROM profiled
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        enrolled_at_date,
        enrolled_at_plus_1d,
        enrollment_term_count,
        last_enrollment_enrolled_at_quintile,
        last_known_enrolled_at_date,
        first_known_enrolled_at_plus_1d,
        DATEDIFF(enrolled_at_date, LAG(enrolled_at_date) OVER (PARTITION BY term ORDER BY score ASC, id)) AS days_since_prev_enrolled_at_date,
        LAG(enrolled_at_plus_1d, 2) OVER (PARTITION BY term ORDER BY score ASC, id) AS prev2_enrolled_at_plus_1d,
        ROUND(enrollment_term_count / NULLIF(SUM(enrollment_term_count) OVER (PARTITION BY last_enrollment_enrolled_at_quintile, TO_DATE(last_enrollment_enrolled_at)), 0), 4) AS enrollment_term_count_share_of_last_enrollment_e,
        COUNT(*) OVER (PARTITION BY last_enrollment_enrolled_at_quintile, TO_DATE(last_enrollment_enrolled_at)) AS rows_in_last_enrollment_enrolled_at_quintile_las,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY last_enrollment_enrolled_at_quintile, TO_DATE(last_enrollment_enrolled_at)), 0), 4) AS score_share_of_last_enrollment_enrolled_at_quint,
        LAG(enrolled_at) OVER (PARTITION BY term ORDER BY score ASC, id) AS prev_enrolled_at
    FROM windowed
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.term,
        s.grade_status,
        s.enrolled_at_date,
        s.enrolled_at_plus_1d,
        s.last_enrollment_enrolled_at_quintile,
        s.last_known_enrolled_at_date,
        s.enrollment_term_count_share_of_last_enrollment_e,
        s.rows_in_last_enrollment_enrolled_at_quintile_las,
        s.prev_enrolled_at,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id
    FROM with_trends s
    JOIN enrollments e ON e.student_id = s.student_id
)
SELECT id, enrollment_id, measure, value
FROM (
    SELECT id, enrollment_id, enrollment_score, enrollment_term_count_share_of_last_enrollment_e
    FROM line_level
) src
UNPIVOT (
    value FOR measure IN (enrollment_score, enrollment_term_count_share_of_last_enrollment_e)
) unp
