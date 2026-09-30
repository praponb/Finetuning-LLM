WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE course_id <= 2
      AND student_id <= 16
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        COUNT(CASE WHEN grade_status IN ('INCOMPLETE', 'FAIL') THEN 1 END) AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM base s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY score_filled DESC, id) AS score_filled_seq,
        SIZE(COLLECT_SET(grade_status) OVER w_enrollment_grade_status_coun) AS distinct_grade_status_in_enrollment_grade_status,
        NTILE(5) OVER (ORDER BY score_filled DESC, id) AS score_filled_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id ORDER BY enrollment_count), 3) AS enrollment_count_percent_rank,
        COUNT(*) OVER (ORDER BY score_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_counts
    WINDOW w_enrollment_grade_status_coun AS (PARTITION BY enrollment_grade_status_count, course_id)
),
joined_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.days_since_enrolled_at,
        s.enrollment_count,
        s.enrollment_grade_status_count,
        s.distinct_grade_status_in_enrollment_grade_status,
        s.enrollment_count_percent_rank,
        c.dept AS course_dept,
        c.credits AS course_credits
    FROM ranked s
    LEFT JOIN courses c ON c.id = s.course_id
)
SELECT
    id,
    student_id,
    grade_status,
    enrollment_count,
    enrollment_grade_status_count,
    course_dept,
    course_credits,
    LOWER(REGEXP_REPLACE(TRIM(course_dept), ' ', '_')) AS course_dept_slug,
    SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix
FROM joined_course
