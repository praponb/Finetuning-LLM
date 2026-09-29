WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status IN ('FAIL', 'PASS')
      AND score < 58.8
),
combined_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        c.dept AS course_dept
    FROM valid_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_enrolled_at AS enrolled_at_seq,
        LAG(enrolled_at) OVER w_all_by_enrolled_at AS prev_enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_enrolled_at
    FROM combined_details_course
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC NULLS FIRST, id)
),
scored AS (
    -- Add calculated columns
    SELECT *, ABS(enrolled_at_seq - 6) AS enrolled_at_seq_distance
    FROM sequenced
    WHERE term IN ('2025T1', '2024T1')
),
latest_per_key AS (
    -- Keep the earliest row per course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        course_dept,
        enrolled_at_seq,
        prev_enrolled_at,
        minutes_since_prev_enrolled_at,
        enrolled_at_seq_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.course_dept,
        s.prev_enrolled_at,
        s.minutes_since_prev_enrolled_at,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM latest_per_key s
    JOIN enrollments e ON e.student_id = s.student_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN grade_status IN ('PASS', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        DATEDIFF(DATE '2026-01-01', prev_enrolled_at) AS days_since_prev_enrolled_at
    FROM with_children_enrollments
    WHERE course_dept = 'Bio'
)
-- Final projection
SELECT
    id,
    student_id,
    grade_status,
    course_dept,
    prev_enrolled_at,
    minutes_since_prev_enrolled_at,
    enrollment_id,
    grade_status_group,
    grade_status_slug,
    SUBSTRING(TRIM(grade_status_group), 1, 2) AS grade_status_group_prefix,
    ROUND((UNIX_TIMESTAMP(prev_enrolled_at) - UNIX_TIMESTAMP(enrollment_enrolled_at)) / 60, 1) AS minutes_enrollment_enrolled_at_to_prev_enrolled_
FROM annotated
WHERE HOUR(prev_enrolled_at) BETWEEN 7 AND 10
ORDER BY grade_status_slug, grade_status
