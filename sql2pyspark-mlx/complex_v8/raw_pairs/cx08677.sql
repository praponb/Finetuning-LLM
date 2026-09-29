WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE score < 73.0
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CASE WHEN score >= 57.8 THEN 'HIGH' WHEN score >= 20.6 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM filtered_enrollments
    WHERE score > 8.7
),
latest AS (
    -- Keep the earliest row per grade_status, course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        score_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status, course_id ORDER BY score, id) AS recency_rank
        FROM derived
    ) ranked
    WHERE recency_rank = 1
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.score_tier,
        c.credits AS course_credits,
        c.course_code,
        c.dept AS course_dept
    FROM latest s
    LEFT JOIN courses c ON c.id = s.course_id
),
stats AS (
    -- Group-level totals
    SELECT
        term,
        student_id,
        course_credits,
        MIN(course_credits) AS min_course_credits,
        ROUND(100 * COUNT(CASE WHEN course_dept = 'Bio' THEN 1 END) / COUNT(*), 1) AS pct_course_dept_bio
    FROM combined_details
    WHERE score_tier <> 'HIGH'
    GROUP BY 1, 2, 3
    HAVING MAX(score) > 16.3
),
with_parent_student AS (
    -- Join students
    SELECT
        s.term,
        s.student_id,
        s.course_credits,
        s.min_course_credits,
        s.pct_course_dept_bio,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort
    FROM stats s
    JOIN students st ON st.id = s.student_id
),
student_cohort_stats AS (
    -- Median min_course_credits per student_cohort
    SELECT student_cohort, PERCENTILE_APPROX(min_course_credits, 0.5) AS median_min_course_credits
    FROM with_parent_student
    GROUP BY student_cohort
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_min_course_credits,
        ROUND(s.min_course_credits - g.median_min_course_credits, 2) AS min_course_credits_minus_median
    FROM with_parent_student s
    JOIN student_cohort_stats g ON g.student_cohort = s.student_cohort
    WHERE s.min_course_credits >= g.median_min_course_credits
)
-- Native SQL UNPIVOT (wide to long)
SELECT term, student_id, metric_name, reading
FROM (
    SELECT term, student_id, course_credits, min_course_credits
    FROM compared
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (course_credits, min_course_credits)
) unp
