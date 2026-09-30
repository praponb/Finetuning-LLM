WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND score < 76.8
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at_dayofweek,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score
    FROM scoped s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
per_group AS (
    -- Summarise per group
    SELECT
        term,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        COUNT(*) AS row_count,
        ROUND(100 * COUNT(CASE WHEN enrollment_grade_status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_enrollment_grade_status_is_n,
        COUNT(DISTINCT enrollment_course_id) AS distinct_enrollment_course_ids,
        COUNT(DISTINCT term) AS distinct_terms,
        MAX(enrollment_score) AS max_enrollment_score
    FROM exploded_detail
    WHERE enrollment_course_id IS NOT NULL
    GROUP BY 1
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE distinct_enrollment_course_ids > (SELECT AVG(distinct_enrollment_course_ids) FROM per_group)
)
SELECT
    term,
    term_list,
    row_count,
    pct_enrollment_grade_status_is_n,
    distinct_enrollment_course_ids,
    distinct_terms,
    max_enrollment_score
FROM filtered
ORDER BY row_count, max_enrollment_score DESC
