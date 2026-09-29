WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        ROUND(score * 0.25, 2) AS score_adj,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-03-08'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        SUM(score_adj) OVER (ORDER BY score, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_adj_sum_4,
        ROUND(AVG(score_adj) OVER (PARTITION BY course_id), 2) AS course_id_avg_score_adj,
        LAST_VALUE(score) OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        ROUND(score_filled - AVG(score_filled) OVER (PARTITION BY course_id), 2) AS score_filled_vs_course_id_avg,
        MAX(score_filled) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at_date ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_filled_max_4
    FROM scoped_enrollments
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score_2,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score
    FROM sequenced s
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM with_counts
    WHERE min_enrollment_score_2 < 80.0
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term <> '2025T1'
              AND e.course_id IS NOT NULL
        )
      AND s.score < 80.2
),
screened AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.dept IN ('CS', 'Math', 'Bio')
        )
),
with_parent AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_filled,
        s.score_adj,
        s.enrolled_at_date,
        s.score_adj_sum_4,
        s.course_id_avg_score_adj,
        s.score_filled_max_4,
        s.min_enrollment_score,
        s.total_enrollment_score,
        s.term_or_unknown,
        s.grade_status_len,
        c.course_code,
        c.credits AS course_credits,
        c.level AS course_level,
        st.student_name,
        st.enrolled_on AS student_enrolled_on
    FROM screened s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    id,
    term,
    score,
    enrolled_at,
    score_filled,
    score_adj,
    enrolled_at_date,
    course_id_avg_score_adj,
    score_filled_max_4,
    min_enrollment_score,
    total_enrollment_score,
    grade_status_len,
    course_code,
    course_level,
    student_enrolled_on,
    CASE WHEN grade_status_len >= 12 THEN 'A' WHEN grade_status_len >= 6 THEN 'B' ELSE 'C' END AS grade_status_len_tier
FROM with_parent
