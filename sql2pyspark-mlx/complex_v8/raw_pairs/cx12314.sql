WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE student_id IS NOT NULL
),
term_stats AS (
    -- Avg score per term
    SELECT term, AVG(score) AS avg_score
    FROM filtered
    GROUP BY term
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM filtered s
    JOIN term_stats g ON g.term = s.term
    WHERE s.score > g.avg_score
),
deduped AS (
    -- Keep the latest row per grade_status, term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        avg_score,
        score_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status, term ORDER BY avg_score DESC, id DESC) AS recency_rank
        FROM above_average
    ) ranked
    WHERE recency_rank = 1
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(avg_score, score_minus_avg) AS greatest_avg_score_score_minus_avg,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM deduped
    WHERE course_id <= 4
),
with_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.avg_score,
        s.greatest_avg_score_score_minus_avg,
        s.grade_status_ref,
        c.level AS course_level,
        st.major AS student_major,
        st.cohort AS student_cohort
    FROM annotated s
    JOIN courses c ON c.id = s.course_id AND c.dept IS NOT NULL
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.level = 'advanced'
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.greatest_avg_score_score_minus_avg,
        s.course_level,
        s.student_cohort,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term,
        e.score AS enrollment_score
    FROM with_details_course s
    JOIN enrollments e ON e.student_id = s.student_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrollment_term, course_level, enrollment_grade_status, score
    FROM detail_rows_enrollments
) src
PIVOT (
    SUM(score)
    FOR enrollment_grade_status IN ('FAIL', 'WITHDRAWN', 'PASS')
)
ORDER BY enrollment_term, course_level
