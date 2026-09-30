WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        is_international
    FROM students
    WHERE cohort NOT IN ('C2023', 'C2021')
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('|', student_name, cohort) AS student_name_cohort_label,
        COALESCE(cohort, 'UNKNOWN') AS cohort_or_unknown,
        SUBSTRING(TRIM(cohort), 1, 2) AS cohort_prefix
    FROM valid
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.cohort,
        s.is_international,
        s.student_name_cohort_label,
        s.cohort_or_unknown,
        s.cohort_prefix,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status
    FROM annotated s
    LEFT JOIN enrollments e ON e.student_id = s.id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.id) AS enrollment_count,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.id) AS total_enrollment_score,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS last_enrollment_enrolled_at
    FROM exploded_detail_enrollments s
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        e.course_id AS enrollment_course_id_2,
        c.credits AS course_credits,
        c.level AS course_level
    FROM profiled s
    JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN courses c ON c.id = s.enrollment_course_id
    WHERE e.course_id <= 4
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_credits ORDER BY last_enrollment_enrolled_at), 3) AS last_enrollment_enrolled_at_percent_rank,
        ROUND(AVG(enrollment_count) OVER (PARTITION BY enrollment_course_id_2, course_level ORDER BY last_enrollment_enrolled_at ASC, id, enrollment_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS enrollment_count_avg_2_prior
    FROM enriched
)
SELECT
    course_credits,
    cohort_or_unknown,
    MAX(last_enrollment_enrolled_at) AS last_last_enrollment_enrolled_at
FROM with_trends
GROUP BY GROUPING SETS ((course_credits, cohort_or_unknown), (course_credits), (cohort_or_unknown), ())
ORDER BY course_credits NULLS LAST, cohort_or_unknown NULLS LAST
