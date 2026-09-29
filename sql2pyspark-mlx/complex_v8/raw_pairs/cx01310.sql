WITH filtered AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        ABS(credits - 3) AS credits_distance,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug,
        LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug
    FROM courses
    WHERE (dept = 'Art' OR credits < 3)
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.dept,
        s.credits,
        s.level,
        s.dept_slug,
        s.course_code_slug,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id
    FROM filtered s
    JOIN enrollments e ON e.course_id = s.id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('|', level, dept_slug) AS level_dept_slug_label,
        ABS(enrollment_score - 49.0) AS enrollment_score_distance,
        INITCAP(TRIM(level)) AS level_clean
    FROM line_level_enrollments
    WHERE course_code_slug IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_code,
        dept,
        credits,
        dept_slug,
        course_code_slug,
        enrollment_id,
        enrollment_score,
        enrollment_student_id,
        level_dept_slug_label,
        enrollment_score_distance,
        level_clean,
        NTILE(10) OVER w_credits_enrollment_term_by_e AS enrollment_score_distance_decile,
        NTH_VALUE(enrollment_enrolled_at, 2) OVER (PARTITION BY credits, enrollment_term ORDER BY enrollment_score_distance ASC, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrollment_enrolled_at,
        FIRST_VALUE(credits) OVER (PARTITION BY credits, enrollment_term ORDER BY enrollment_score_distance ASC, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_credits,
        ROUND(PERCENT_RANK() OVER (PARTITION BY credits, enrollment_term ORDER BY enrollment_score_distance ASC), 3) AS enrollment_score_distance_percent_rank
    FROM scored
    WINDOW w_credits_enrollment_term_by_e AS (PARTITION BY credits, enrollment_term ORDER BY enrollment_score_distance ASC, id, enrollment_id)
),
summary AS (
    -- Group-level totals
    SELECT
        first_credits,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(dept)) AS dept_list,
        COUNT(*) AS row_count,
        ROUND(STDDEV(enrollment_score_distance_decile), 2) AS stddev_enrollment_score_distance_decile
    FROM with_trends
    GROUP BY first_credits
),
filtered_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE first_credits > (SELECT AVG(first_credits) FROM summary)
)
-- Final projection
SELECT
    first_credits,
    distinct_ids,
    row_count,
    stddev_enrollment_score_distance_decile
FROM filtered_2
ORDER BY stddev_enrollment_score_distance_decile DESC, distinct_ids DESC NULLS FIRST, first_credits
LIMIT 10
