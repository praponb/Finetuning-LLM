WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        credits,
        level,
        COALESCE(dept, 'UNKNOWN') AS dept_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug,
        COALESCE(credits, 0) AS credits_filled
    FROM courses
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', dept_slug, course_code) AS dept_slug_course_code_label,
        ABS(credits_filled - 2) AS credits_filled_distance,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown
    FROM prepared
    WHERE course_code IS NOT NULL
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.credits,
        s.level,
        s.dept_or_unknown,
        s.credits_filled,
        s.credits_filled_distance,
        s.level_or_unknown,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.student_id AS enrollment_student_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM annotated s
    JOIN enrollments e ON e.course_id = s.id AND e.score >= 20.6
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        credits,
        level,
        dept_or_unknown,
        credits_filled,
        level_or_unknown,
        enrollment_id,
        enrollment_score,
        enrollment_student_id,
        enrollment_term,
        SIZE(COLLECT_SET(level_or_unknown) OVER (PARTITION BY credits_filled_distance)) AS distinct_level_or_unknown_in_credits_filled_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', enrollment_enrolled_at), credits_filled ORDER BY credits, id, enrollment_id), 3) AS credits_percent_rank,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', enrollment_enrolled_at), credits_filled ORDER BY credits) AS credits_dense_rank,
        MAX(enrollment_score) OVER w_credits_filled_distance AS credits_filled_distance_max_enrollment_score
    FROM exploded_detail
    WINDOW w_credits_filled_distance AS (PARTITION BY credits_filled_distance)
),
level_stats AS (
    -- Max credits_dense_rank per level
    SELECT level, MAX(credits_dense_rank) AS max_credits_dense_rank
    FROM sequenced
    GROUP BY level
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_credits_dense_rank,
        ROUND(s.credits_dense_rank - g.max_credits_dense_rank, 2) AS credits_dense_rank_minus_max
    FROM sequenced s
    JOIN level_stats g ON g.level = s.level
    WHERE s.credits_dense_rank = g.max_credits_dense_rank
)
SELECT
    id,
    credits,
    level,
    dept_or_unknown,
    credits_filled,
    level_or_unknown,
    enrollment_id,
    enrollment_student_id,
    enrollment_term,
    distinct_level_or_unknown_in_credits_filled_dist,
    credits_percent_rank,
    credits_dense_rank,
    credits_filled_distance_max_enrollment_score
FROM compared
ORDER BY level NULLS LAST
