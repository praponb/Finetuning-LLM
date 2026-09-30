WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score >= 8.7
),
with_parent AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        c.credits AS course_credits,
        st.student_name
    FROM scoped_enrollments s
    JOIN courses c ON c.id = s.course_id AND LENGTH(TRIM(c.course_code)) >= 5
    JOIN students st ON st.id = s.student_id
    WHERE c.credits >= 2
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY course_id, course_credits ORDER BY enrolled_at DESC) AS enrolled_at_dense_rank,
        LEAD(course_credits) OVER (PARTITION BY course_credits ORDER BY score ASC, id) AS next_course_credits,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_credits ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        ROUND(AVG(course_credits) OVER (PARTITION BY course_credits ORDER BY score ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS course_credits_avg_4,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY course_credits ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        SUM(score) OVER (PARTITION BY student_id, term) AS student_id_term_sum_score
    FROM with_parent
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE term IS NOT NULL) AS enrollment_term_count,
        COUNT(*) AS enrollment_count,
        COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM sequenced s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
screened AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND UPPER(TRIM(c.course_code)) LIKE 'M%'
              AND c.level = 'intro'
        )
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at
    FROM screened s
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    course_credits,
    student_name,
    enrolled_at_dense_rank,
    next_course_credits,
    course_credits_avg_4,
    student_id_term_sum_score,
    enrollment_count,
    enrollment_count_2,
    total_enrollment_score,
    avg_enrollment_score,
    last_enrollment_enrolled_at,
    INITCAP(TRIM(term)) AS term_clean
FROM with_counts
ORDER BY score DESC NULLS FIRST, student_id_term_sum_score DESC
