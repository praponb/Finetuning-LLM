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
        LENGTH(TRIM(term)) AS term_len,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE (term = '2025T1' OR grade_status IN ('INCOMPLETE', 'PASS', 'WITHDRAWN'))
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at_label,
        ROUND(AVG(score) OVER w_student_id, 2) AS student_id_avg_score,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY score DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        SUM(term_len) OVER (PARTITION BY student_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_term_len
    FROM cleaned
    WINDOW w_student_id AS (PARTITION BY student_id)
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_id,
        s.enrolled_at,
        s.minutes_since_prev_enrolled_at,
        s.running_term_len,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id,
        e.term AS enrollment_term
    FROM sequenced s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(enrollment_grade_status), 1, 4) AS enrollment_grade_status_prefix
    FROM exploded_detail
),
enrollment_student_id_stats AS (
    -- Median running_term_len per enrollment_student_id
    SELECT enrollment_student_id, PERCENTILE_APPROX(running_term_len, 0.5) AS median_running_term_len
    FROM labeled
    GROUP BY enrollment_student_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_running_term_len,
        ROUND(s.running_term_len - g.median_running_term_len, 2) AS running_term_len_minus_median
    FROM labeled s
    JOIN enrollment_student_id_stats g ON g.enrollment_student_id = s.enrollment_student_id
    WHERE s.running_term_len >= g.median_running_term_len
)
-- Final projection
SELECT
    id,
    running_term_len,
    enrollment_id,
    enrollment_grade_status,
    enrollment_term,
    enrollment_grade_status_prefix,
    median_running_term_len,
    running_term_len_minus_median,
    SUBSTRING(TRIM(enrollment_grade_status), 1, 4) AS enrollment_grade_status_prefix_2
FROM vs_group
WHERE enrollment_grade_status IS NOT NULL
ORDER BY running_term_len_minus_median DESC
