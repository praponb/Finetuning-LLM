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
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term = '2024T2'
        )
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) AS enrollment_count,
        MAX(score) AS max_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.max_enrollment_score
    FROM without_matches s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        enrolled_at_quarter,
        enrolled_at_date,
        max_enrollment_score,
        ROW_NUMBER() OVER w_course_id_grade_status_or_un AS seq_no,
        ROW_NUMBER() OVER (ORDER BY enrolled_at_quarter ASC, id) AS seq_no_2,
        LAG(enrolled_at, 2) OVER (PARTITION BY student_id, grade_status_or_unknown ORDER BY max_enrollment_score DESC, id) AS prev2_enrolled_at,
        NTILE(10) OVER (ORDER BY enrolled_at_quarter ASC, id) AS enrolled_at_quarter_decile
    FROM with_stats
    WINDOW w_course_id_grade_status_or_un AS (PARTITION BY course_id, grade_status_or_unknown ORDER BY enrolled_at_date NULLS LAST, id)
)
SELECT
    term,
    seq_no,
    student_id,
    GROUPING(term) AS is_total_term,
    ROUND(SUM(max_enrollment_score), 2) AS total_max_enrollment_score
FROM windowed
GROUP BY CUBE(term, seq_no, student_id)
ORDER BY term NULLS LAST, seq_no NULLS LAST, student_id NULLS LAST
