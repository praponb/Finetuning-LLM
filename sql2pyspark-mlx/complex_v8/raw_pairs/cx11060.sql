WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        ABS(score - 50.9) AS score_distance,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND enrolled_at < TIMESTAMP '2025-02-24 14:00:00'
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.enrolled_at < TIMESTAMP '2025-02-18 11:00:00'
        )
      AND s.term_slug IS NOT NULL
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.term_prefix,
        s.score_distance,
        s.term_slug,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term
    FROM with_activity s
    LEFT JOIN enrollments e ON e.course_id = s.course_id AND e.student_id IS NOT NULL
),
per_group AS (
    -- Group-level totals
    SELECT
        enrollment_id,
        SUM(score_distance) FILTER (WHERE enrollment_term IN ('2025T1', '2024T2')) AS score_distance_enrollment_term_in_2025t1_20,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE enrollment_term = '2024T2') AS n_enrollment_term_2024t2,
        ROUND(SUM(score_distance) / NULLIF(SUM(score), 0), 3) AS score_distance_to_score_ratio
    FROM line_level
    GROUP BY enrollment_id
    HAVING AVG(score_distance) > 35.0
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(n_enrollment_term_2024t2) OVER (PARTITION BY n_enrollment_term_2024t2 ORDER BY cnt ASC, enrollment_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS n_enrollment_term_2024t2_max_2,
        ROUND(CUME_DIST() OVER (ORDER BY score_distance_to_score_ratio), 3) AS score_distance_to_score_ratio_cume_dist
    FROM per_group
),
joined AS (
    -- Join enrollments
    SELECT
        s.enrollment_id,
        s.score_distance_enrollment_term_in_2025t1_20,
        s.cnt,
        s.n_enrollment_term_2024t2,
        s.score_distance_to_score_ratio,
        s.n_enrollment_term_2024t2_max_2,
        e.score AS enrollment_score,
        e.term AS enrollment_term,
        st.is_international AS student_is_international,
        st.cohort AS student_cohort
    FROM ranked s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN students st ON st.id = e.student_id
)
SELECT
    enrollment_id,
    score_distance_enrollment_term_in_2025t1_20,
    cnt,
    score_distance_to_score_ratio,
    n_enrollment_term_2024t2_max_2,
    enrollment_score,
    enrollment_term,
    student_is_international,
    student_cohort,
    COALESCE(enrollment_score, 0.0) AS enrollment_score_filled,
    LOWER(TRIM(student_cohort)) AS student_cohort_clean
FROM joined
