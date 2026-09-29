WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM valid_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status IS NOT NULL
        )
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label
    FROM with_activity
),
scored AS (
    -- Derive helper columns
    SELECT *, TO_DATE(enrolled_at) AS enrolled_at_date
    FROM tagged
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_grade_status_by_enrolled_at AS enrolled_at_seq,
        DATEDIFF(enrolled_at_date, LAG(enrolled_at_date) OVER (ORDER BY days_since_enrolled_at ASC, id)) AS days_since_prev_enrolled_at_date,
        SIZE(COLLECT_SET(course_id) OVER w_student_id) AS distinct_course_id_in_student_id,
        RANK() OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC) AS enrolled_at_rank,
        ROUND(days_since_enrolled_at - AVG(days_since_enrolled_at) OVER (PARTITION BY student_id), 2) AS days_since_enrolled_at_vs_student_id_avg,
        LAG(days_since_enrolled_at) OVER w_all_by_days_since_enrolled_a AS prev_days_since_enrolled_at
    FROM scored
    WINDOW w_grade_status_by_enrolled_at AS (PARTITION BY grade_status ORDER BY enrolled_at DESC, id), w_student_id AS (PARTITION BY student_id), w_all_by_days_since_enrolled_a AS (ORDER BY days_since_enrolled_at ASC, id)
),
student_id_stats AS (
    -- Avg grade_status_len per student_id
    SELECT student_id, AVG(grade_status_len) AS avg_grade_status_len
    FROM metrics
    GROUP BY student_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_grade_status_len,
        ROUND(s.grade_status_len - g.avg_grade_status_len, 2) AS grade_status_len_minus_avg
    FROM metrics s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.grade_status_len > g.avg_grade_status_len
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    days_since_enrolled_at,
    grade_status_len,
    term_grade_status_label,
    enrolled_at_date,
    days_since_prev_enrolled_at_date,
    distinct_course_id_in_student_id,
    enrolled_at_rank,
    days_since_enrolled_at_vs_student_id_avg,
    prev_days_since_enrolled_at,
    grade_status_len_minus_avg,
    INITCAP(TRIM(term_grade_status_label)) AS term_grade_status_label_clean,
    COALESCE(enrolled_at_seq, 0) AS enrolled_at_seq_filled
FROM outliers
WHERE distinct_course_id_in_student_id > 2
ORDER BY id, prev_days_since_enrolled_at
