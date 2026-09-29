WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE term IS NOT NULL
),
enriched_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.score_filled,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort,
        c.dept AS course_dept,
        c.credits AS course_credits
    FROM prepared s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
),
student_cohort_stats AS (
    -- Median score_filled per student_cohort
    SELECT student_cohort, PERCENTILE_APPROX(score_filled, 0.5) AS median_score_filled
    FROM enriched_student
    GROUP BY student_cohort
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score_filled,
        ROUND(s.score_filled - g.median_score_filled, 2) AS score_filled_minus_median
    FROM enriched_student s
    JOIN student_cohort_stats g ON g.student_cohort = s.student_cohort
    WHERE s.score_filled >= g.median_score_filled
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN course_dept IN ('Bio', 'Art') THEN 'group_a' WHEN course_dept = 'Biz' THEN 'group_b' ELSE 'other' END AS course_dept_group,
        ROUND(median_score_filled * 1.5, 2) AS median_score_filled_adj
    FROM outliers
)
-- Final projection
SELECT
    id,
    course_id,
    enrolled_at,
    score_filled,
    course_dept,
    course_credits,
    median_score_filled,
    score_filled_minus_median,
    median_score_filled_adj
FROM tagged
WHERE score_filled_minus_median <= 20.0
ORDER BY score_filled, median_score_filled, id
LIMIT 10
