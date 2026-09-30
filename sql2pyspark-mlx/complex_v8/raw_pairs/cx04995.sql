WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        CASE WHEN term IN ('2025T2', '2025T1') THEN 'group_a' WHEN term = '2024T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-19 15:00:00'
),
enriched_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.score_filled,
        s.term_group,
        c.course_code,
        c.credits AS course_credits,
        st.is_international AS student_is_international
    FROM recent_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY MONTH(enrolled_at), student_is_international ORDER BY score_filled NULLS LAST) AS score_filled_dense_rank,
        ROW_NUMBER() OVER w_enrolled_at_month_student_is_2 AS rn
    FROM enriched_course
    WINDOW w_enrolled_at_month_student_is_2 AS (PARTITION BY MONTH(enrolled_at), student_is_international ORDER BY score_filled NULLS LAST, id)
)
SELECT
    course_id,
    COALESCE(term_group, 'ALL') AS term_group,
    GROUPING(course_id) AS is_total_course_id,
    SUM(rn) AS total_rn
FROM with_windows
GROUP BY GROUPING SETS ((course_id, term_group), (course_id), ())
ORDER BY course_id NULLS LAST, term_group NULLS LAST
