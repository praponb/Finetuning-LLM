WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        INITCAP(TRIM(grade_status)) AS grade_status_clean,
        CASE WHEN score >= 62.5 THEN 'HIGH' WHEN score >= 17.4 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE term = '2025T2'
),
joined AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.score_tier,
        st.is_international AS student_is_international,
        st.cohort AS student_cohort,
        c.credits AS course_credits
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
    WHERE st.cohort NOT IN ('C2023', 'C2024')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY student_is_international, student_cohort ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_cume_dist,
        SUM(course_credits) OVER w_enrolled_at_year AS enrolled_at_year_sum_course_credits,
        MAX(course_credits) OVER (PARTITION BY YEAR(enrolled_at)) AS enrolled_at_year_max_course_credits,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id, course_id ORDER BY course_credits ASC NULLS LAST), 3) AS course_credits_cume_dist,
        LAG(enrolled_at) OVER (PARTITION BY student_cohort, term ORDER BY course_credits, id) AS prev_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY student_id, course_id ORDER BY course_credits ASC NULLS LAST, id) AS course_credits_dense_rank
    FROM joined
    WINDOW w_enrolled_at_year AS (PARTITION BY YEAR(enrolled_at))
)
-- Final projection
SELECT
    id,
    enrolled_at,
    score_tier,
    student_is_international,
    student_cohort,
    course_credits,
    enrolled_at_cume_dist,
    enrolled_at_year_sum_course_credits,
    course_credits_cume_dist,
    prev_enrolled_at,
    course_credits_dense_rank,
    ROUND((UNIX_TIMESTAMP(prev_enrolled_at) - UNIX_TIMESTAMP(enrolled_at)) / 3600, 1) AS hours_enrolled_at_to_prev_enrolled_at,
    LOWER(TRIM(score_tier)) AS score_tier_clean
FROM with_windows
ORDER BY enrolled_at DESC, course_credits DESC, id
LIMIT 10
