WITH prepared_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled,
        CASE WHEN credits >= 3 THEN 'A' WHEN credits >= 2 THEN 'B' ELSE 'C' END AS credits_tier
    FROM courses
    WHERE level IN ('core', 'intro')
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.dept,
        s.credits,
        s.level,
        s.credits_filled,
        s.credits_tier,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at
    FROM prepared_courses s
    JOIN enrollments e ON e.course_id = s.id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        dept,
        credits,
        level,
        credits_filled,
        credits_tier,
        enrollment_id,
        enrollment_term,
        enrollment_grade_status,
        enrollment_enrolled_at,
        enrollment_score - LAG(enrollment_score) OVER (PARTITION BY credits_tier, TO_DATE(enrollment_enrolled_at) ORDER BY enrollment_score, id, enrollment_id) AS enrollment_score_change,
        SIZE(COLLECT_SET(level) OVER (PARTITION BY enrollment_term, enrollment_grade_status)) AS distinct_level_in_enrollment_term_enrollment_gra
    FROM detail_rows
    WINDOW w_credits_tier_enrollment_enro AS (PARTITION BY credits_tier, TO_DATE(enrollment_enrolled_at) ORDER BY enrollment_score, id, enrollment_id)
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.dept,
        s.credits,
        s.level,
        s.credits_filled,
        s.credits_tier,
        s.enrollment_id,
        s.enrollment_enrolled_at,
        s.enrollment_score_change,
        s.distinct_level_in_enrollment_term_enrollment_gra,
        e.id AS enrollment_id_2,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at_2,
        e.grade_status AS enrollment_grade_status_2,
        e.term AS enrollment_term_2
    FROM windowed s
    LEFT JOIN enrollments e ON e.course_id = s.id
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        dept,
        credits,
        credits_filled,
        credits_tier,
        enrollment_id,
        enrollment_enrolled_at,
        enrollment_id_2,
        enrollment_score,
        enrollment_term_2,
        SUBSTRING(TRIM(level), 1, 4) AS level_prefix,
        ABS(enrollment_score - 49.0) AS enrollment_score_distance,
        CASE WHEN enrollment_score >= 77.0 THEN 'HIGH' WHEN enrollment_score >= 20.6 THEN 'MEDIUM' ELSE 'LOW' END AS enrollment_score_tier
    FROM detail_rows_enrollments
    WHERE enrollment_id_2 >= 51
)
SELECT
    id,
    dept,
    credits_filled,
    credits_tier,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_id_2,
    enrollment_score,
    enrollment_score_tier
FROM scored
