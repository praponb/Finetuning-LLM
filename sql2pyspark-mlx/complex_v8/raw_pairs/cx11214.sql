WITH recent AS (
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
),
with_parent_student AS (
    -- Enrich with dimension data
    SELECT s.*, st.enrolled_on AS student_enrolled_on, st.cohort AS student_cohort
    FROM recent s
    JOIN students st ON st.id = s.student_id
),
student_cohort_stats AS (
    -- Median score per student_cohort
    SELECT student_cohort, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM with_parent_student
    GROUP BY student_cohort
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM with_parent_student s
    JOIN student_cohort_stats g ON g.student_cohort = s.student_cohort
    WHERE s.score >= g.median_score
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT student_enrolled_on, term, grade_status, score_minus_median
    FROM above_average
) src
PIVOT (
    SUM(score_minus_median) AS amount, MAX(score_minus_median) AS max_v
    FOR grade_status IN ('FAIL', 'PASS')
)
ORDER BY student_enrolled_on, term
