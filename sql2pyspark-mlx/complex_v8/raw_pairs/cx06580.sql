WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix,
        ROUND(score * 0.9, 2) AS score_adj
    FROM enrollments
    WHERE term NOT IN ('2025T2', '2025T1')
),
enriched_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_prefix,
        s.score_adj,
        c.credits AS course_credits,
        c.level AS course_level,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort
    FROM base_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(student_gpa / NULLIF(SUM(student_gpa) OVER (PARTITION BY student_id), 0), 4) AS student_gpa_share_of_student_id,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER (PARTITION BY student_id), 0), 4) AS course_credits_share_of_student_id,
        MAX(student_gpa) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_student_gpa_so_far,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY student_id)) AS distinct_course_id_in_student_id,
        MIN(student_gpa) OVER w_student_id AS student_id_min_student_gpa,
        LAG(student_gpa) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_student_gpa
    FROM enriched_course
    WINDOW w_student_id AS (PARTITION BY student_id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        term,
        course_credits,
        student_id,
        MAX(enrolled_at) AS last_enrolled_at,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        MAX(course_credits) AS max_course_credits,
        ARRAY_SORT(COLLECT_SET(student_cohort)) AS student_cohort_list,
        COUNT(*) AS row_count,
        MIN(enrolled_at) AS first_enrolled_at
    FROM with_windows
    GROUP BY term, course_credits, student_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN course_credits >= 3 THEN 'large' WHEN course_credits >= 2 THEN 'medium' ELSE 'small' END AS course_credits_tier,
        ROUND((UNIX_TIMESTAMP(last_enrolled_at) - UNIX_TIMESTAMP(first_enrolled_at)) / 60, 1) AS minutes_first_enrolled_at_to_last_enrolled_at,
        COALESCE(course_credits, 0) AS course_credits_filled
    FROM grouped
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(course_credits_tier), 1, 3) AS course_credits_tier_prefix
    FROM scored
),
labeled AS (
    -- Add calculated columns
    SELECT
        term,
        course_credits,
        student_id,
        last_enrolled_at,
        distinct_course_ids,
        max_course_credits,
        student_cohort_list,
        row_count,
        course_credits_tier,
        minutes_first_enrolled_at_to_last_enrolled_at,
        DATEDIFF(DATE '2025-12-31', last_enrolled_at) AS days_since_last_enrolled_at,
        QUARTER(first_enrolled_at) AS first_enrolled_at_quarter,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM derived
)
-- Final projection
SELECT
    term,
    course_credits,
    max_course_credits,
    student_cohort_list,
    row_count,
    minutes_first_enrolled_at_to_last_enrolled_at,
    days_since_last_enrolled_at,
    first_enrolled_at_quarter,
    term_slug
FROM labeled
WHERE distinct_course_ids < 8
ORDER BY course_credits, first_enrolled_at_quarter DESC NULLS FIRST
