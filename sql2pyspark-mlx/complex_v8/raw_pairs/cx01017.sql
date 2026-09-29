WITH scoped AS (
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
    WHERE grade_status NOT IN ('PASS', 'FAIL')
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        st.enrolled_on AS student_enrolled_on,
        st.major AS student_major,
        st.cohort AS student_cohort
    FROM scoped s
    JOIN students st ON st.id = s.student_id
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        student_enrolled_on,
        student_cohort,
        FIRST_VALUE(student_enrolled_on) OVER (PARTITION BY term ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_student_enrolled_on,
        DENSE_RANK() OVER (PARTITION BY term ORDER BY score DESC, id) AS score_dense_rank
    FROM without_matches
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        student_enrolled_on,
        score_dense_rank,
        DATEDIFF(student_enrolled_on, LAG(student_enrolled_on) OVER (PARTITION BY student_id, term ORDER BY score DESC, id)) AS days_since_prev_student_enrolled_on,
        RANK() OVER (PARTITION BY student_id, term ORDER BY score DESC, id) AS score_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id
    FROM with_trends
),
without_matches_2 AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND TO_DATE(e.enrolled_at) < DATE '2025-03-08'
        )
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    score_dense_rank,
    days_since_prev_student_enrolled_on,
    score_rank,
    score_share_of_course_id,
    DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
FROM without_matches_2
