WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 73.0 THEN 'large' WHEN score >= 38.5 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE course_id >= 3
),
student_id_stats AS (
    -- Avg score per student_id
    SELECT student_id, AVG(score) AS avg_score
    FROM base
    GROUP BY student_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM base s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score > g.avg_score
),
with_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_tier,
        s.avg_score,
        s.score_minus_avg,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort,
        st.enrolled_on AS student_enrolled_on,
        c.dept AS course_dept,
        c.credits AS course_credits
    FROM outliers s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
),
latest_per_key AS (
    -- Keep the latest row per student_cohort, score_tier
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_tier,
        avg_score,
        score_minus_avg,
        student_gpa,
        student_cohort,
        student_enrolled_on,
        course_dept,
        course_credits
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_cohort, score_tier ORDER BY avg_score DESC, id DESC) AS recency_rank
        FROM with_details_student
    ) ranked
    WHERE recency_rank = 1
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.avg_score,
        s.student_gpa,
        s.student_enrolled_on,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term,
        e.score AS enrollment_score
    FROM latest_per_key s
    JOIN enrollments e ON e.student_id = s.student_id
),
totals AS (
    -- Summarise per group
    SELECT
        enrollment_id,
        TO_DATE(enrollment_enrolled_at) AS enrollment_enrolled_at_day,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        COUNT(*) AS n_rows
    FROM detail_rows_enrollments
    GROUP BY 1, 2
)
-- Final projection
SELECT
    enrollment_id,
    enrollment_enrolled_at_day,
    distinct_course_ids,
    distinct_student_ids,
    CASE WHEN distinct_student_ids >= 7 THEN 'HIGH' WHEN distinct_student_ids >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_student_ids_tier,
    COALESCE(distinct_student_ids, 0) AS distinct_student_ids_filled
FROM totals
WHERE enrollment_enrolled_at_day IS NOT NULL
ORDER BY enrollment_enrolled_at_day DESC, distinct_student_ids_filled NULLS LAST
