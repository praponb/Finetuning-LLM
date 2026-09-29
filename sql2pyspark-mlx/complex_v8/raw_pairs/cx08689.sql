WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix
    FROM enrollments
    WHERE grade_status IN ('INCOMPLETE', 'PASS', 'WITHDRAWN')
      AND score >= 38.1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(enrolled_at) OVER w_student_id_by_enrolled_at AS prev_enrolled_at,
        NTILE(10) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS enrolled_at_decile,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY student_id, course_id)) AS distinct_grade_status_in_student_id_course_id
    FROM prepared_enrollments
    WINDOW w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at, id)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        MAX(enrolled_at) AS last_enrollment_enrolled_at_2,
        MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        e.last_enrollment_enrolled_at_2,
        e.first_enrollment_enrolled_at
    FROM with_windows s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
SELECT
    enrolled_at_decile,
    student_id,
    COALESCE(grade_status, 'ALL') AS grade_status,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT id) AS distinct_ids
FROM summarized_rows
GROUP BY ROLLUP(enrolled_at_decile, student_id, grade_status)
ORDER BY enrolled_at_decile NULLS LAST, student_id NULLS LAST, grade_status NULLS LAST
