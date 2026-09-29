WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2024T1', '2025T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
    WHERE term = '2025T1'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.term_group,
        s.score_adj,
        st.cohort AS student_cohort,
        c.dept AS course_dept,
        c.course_code
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM enriched
    WHERE score_adj > (SELECT MIN(score_adj) FROM enriched)
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    grade_status,
    course_code
FROM qualified
WHERE course_dept = 'Biz'
ORDER BY course_code NULLS LAST, id
LIMIT 10
