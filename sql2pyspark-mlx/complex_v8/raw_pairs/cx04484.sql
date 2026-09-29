WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
),
with_parent AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        st.cohort AS student_cohort,
        st.enrolled_on AS student_enrolled_on,
        c.level AS course_level,
        c.credits AS course_credits
    FROM valid_enrollments s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
course_level_stats AS (
    -- Avg course_credits per course_level
    SELECT course_level, AVG(course_credits) AS avg_course_credits
    FROM with_parent
    GROUP BY course_level
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_course_credits,
        ROUND(s.course_credits - g.avg_course_credits, 2) AS course_credits_minus_avg
    FROM with_parent s
    JOIN course_level_stats g ON g.course_level = s.course_level
    WHERE s.course_credits > g.avg_course_credits
),
enriched AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.student_cohort,
        s.student_enrolled_on,
        s.course_level,
        s.course_credits,
        s.course_credits_minus_avg,
        st.is_international AS student_is_international
    FROM compared s
    LEFT JOIN students st ON st.id = s.student_id
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        MAX(score) AS max_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at,
        e.last_enrollment_enrolled_at,
        e.max_enrollment_score
    FROM enriched s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(course_credits_minus_avg / NULLIF(max_enrollment_score, 0), 3) AS course_credits_minus_avg_per_max_enrollment_scor,
        course_credits - course_credits_minus_avg AS course_credits_minus_course_credits_minus_avg
    FROM with_counts
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        course_id,
        enrolled_at,
        student_cohort,
        student_enrolled_on,
        course_level,
        course_credits,
        course_credits_minus_avg,
        first_enrollment_enrolled_at,
        last_enrollment_enrolled_at,
        course_credits_minus_avg_per_max_enrollment_scor,
        course_credits_minus_course_credits_minus_avg,
        ROUND(CUME_DIST() OVER w_all_by_max_enrollment_score, 3) AS max_enrollment_score_cume_dist,
        ROUND(PERCENT_RANK() OVER w_course_credits_by_enrolled_a, 3) AS enrolled_at_percent_rank,
        COUNT(*) OVER (PARTITION BY student_cohort) AS rows_in_student_cohort
    FROM scored
    WINDOW w_all_by_max_enrollment_score AS (ORDER BY max_enrollment_score DESC), w_course_credits_by_enrolled_a AS (PARTITION BY course_credits ORDER BY enrolled_at DESC, id)
)
SELECT *
FROM (
    SELECT course_id, course_level, student_cohort, course_credits_minus_avg
    FROM with_windows
) src
PIVOT (
    MIN(course_credits_minus_avg)
    FOR student_cohort IN ('C2022', 'C2024', 'C2021')
)
ORDER BY course_id, course_level
