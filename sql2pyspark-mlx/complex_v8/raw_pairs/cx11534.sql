WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        level,
        UPPER(TRIM(dept)) AS dept_clean
    FROM courses
    WHERE dept <> 'Biz'
      AND level IN ('intro', 'advanced')
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.level,
        s.dept_clean,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM recent s
    JOIN enrollments e ON e.course_id = s.id AND e.student_id IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per enrollment_student_id, enrollment_grade_status
    SELECT
        id,
        course_code,
        level,
        dept_clean,
        enrollment_id,
        enrollment_grade_status,
        enrollment_student_id,
        enrollment_enrolled_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY enrollment_student_id, enrollment_grade_status ORDER BY enrollment_enrolled_at DESC, id DESC, enrollment_id DESC) AS row_num
        FROM with_children
    ) ranked
    WHERE row_num = 1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_enrollment_enrolled_at_month AS enrollment_enrolled_at_rank,
        LAG(enrollment_enrolled_at) OVER (PARTITION BY level ORDER BY enrollment_enrolled_at ASC, id, enrollment_id) AS prev_enrollment_enrolled_at
    FROM unique_rows
    WINDOW w_enrollment_enrolled_at_month AS (PARTITION BY DATE_TRUNC('MONTH', enrollment_enrolled_at) ORDER BY enrollment_enrolled_at)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(enrollment_enrolled_at) - UNIX_TIMESTAMP(LAG(enrollment_enrolled_at) OVER w_level_enrollment_enrolled_at)) / 60, 1) AS minutes_since_prev_enrollment_enrolled_at,
        ROUND(enrollment_enrolled_at_rank - AVG(enrollment_enrolled_at_rank) OVER (PARTITION BY enrollment_student_id, level), 2) AS enrollment_enrolled_at_rank_vs_enrollment_studen,
        COUNT(*) OVER (PARTITION BY enrollment_student_id, level) AS rows_in_enrollment_student_id_level,
        ROUND((UNIX_TIMESTAMP(enrollment_enrolled_at) - UNIX_TIMESTAMP(LAG(enrollment_enrolled_at) OVER w_enrollment_student_id_by_enr)) / 60, 1) AS minutes_since_prev_enrollment_enrolled_at_2
    FROM sequenced
    WINDOW w_level_enrollment_enrolled_at AS (PARTITION BY level, enrollment_enrolled_at_rank ORDER BY enrollment_enrolled_at ASC, id, enrollment_id), w_enrollment_student_id_by_enr AS (PARTITION BY enrollment_student_id ORDER BY enrollment_enrolled_at DESC, id, enrollment_id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', enrollment_enrolled_at) AS days_since_enrollment_enrolled_at,
        CONCAT(level, '#', CAST(id AS STRING)) AS level_ref,
        DATE_TRUNC('MONTH', enrollment_enrolled_at) AS enrollment_enrolled_at_month
    FROM with_windows
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        course_code,
        level,
        dept_clean,
        enrollment_id,
        enrollment_grade_status,
        enrollment_student_id,
        enrollment_enrolled_at,
        enrollment_enrolled_at_rank,
        prev_enrollment_enrolled_at,
        minutes_since_prev_enrollment_enrolled_at,
        days_since_enrollment_enrolled_at,
        level_ref,
        enrollment_enrolled_at_month,
        LENGTH(TRIM(course_code)) AS course_code_len
    FROM derived
    WHERE days_since_enrollment_enrolled_at < 560
)
SELECT
    id,
    course_code,
    level,
    dept_clean,
    enrollment_id,
    enrollment_grade_status,
    enrollment_enrolled_at,
    enrollment_enrolled_at_rank,
    prev_enrollment_enrolled_at,
    days_since_enrollment_enrolled_at,
    level_ref,
    enrollment_enrolled_at_month,
    course_code_len,
    COALESCE(course_code_len, 0) AS course_code_len_filled
FROM annotated
ORDER BY days_since_enrollment_enrolled_at DESC, enrollment_grade_status DESC, id, enrollment_id
LIMIT 5
