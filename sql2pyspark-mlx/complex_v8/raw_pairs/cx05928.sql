WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE student_id >= 4
      AND MONTH(enrolled_at) IN (2, 3)
),
joined_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.grade_status_or_unknown,
        c.dept AS course_dept,
        c.credits AS course_credits,
        c.course_code
    FROM cleaned s
    LEFT JOIN courses c ON c.id = s.course_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY course_credits ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY course_credits), 2) AS course_credits_vs_course_credits_avg,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_credits)) AS distinct_term_in_course_credits,
        SIZE(COLLECT_SET(course_dept) OVER (PARTITION BY term)) AS distinct_course_dept_in_term,
        LAG(course_credits, 3) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id) AS prev3_course_credits,
        COUNT(*) OVER (PARTITION BY course_credits) AS rows_in_course_credits,
        MIN(course_credits) OVER (PARTITION BY term) AS term_min_course_credits,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_all_by_course_credits)) / 60, 1) AS minutes_since_prev_enrolled_at,
        SUM(course_credits) OVER (PARTITION BY term) AS term_sum_course_credits,
        MAX(course_credits) OVER (PARTITION BY term) AS term_max_course_credits,
        ROW_NUMBER() OVER w_course_id_by_enrolled_at AS seq_no,
        MIN(course_credits) OVER (PARTITION BY course_credits) AS course_credits_min_course_credits
    FROM joined_course
    WINDOW w_all_by_course_credits AS (ORDER BY course_credits DESC, id), w_course_id_by_enrolled_at AS (PARTITION BY course_id ORDER BY enrolled_at DESC, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    grade_status_or_unknown,
    course_dept,
    course_credits,
    course_code,
    running_count,
    course_credits_vs_course_credits_avg,
    distinct_term_in_course_credits,
    distinct_course_dept_in_term,
    prev3_course_credits,
    rows_in_course_credits,
    term_min_course_credits,
    term_sum_course_credits,
    term_max_course_credits,
    seq_no,
    LENGTH(TRIM(grade_status_or_unknown)) AS grade_status_or_unknown_len,
    term_sum_course_credits + running_count AS term_sum_course_credits_plus_running_count
FROM with_windows
WHERE course_credits_min_course_credits <= 32
ORDER BY term, course_id
