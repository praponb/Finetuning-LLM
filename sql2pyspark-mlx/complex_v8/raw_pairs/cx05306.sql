WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        MONTH(enrolled_at) AS enrolled_at_month,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref
    FROM enrollments
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m,
        enrolled_at_month + score AS enrolled_at_month_plus_score
    FROM scoped_enrollments
    WHERE term_ref IS NOT NULL
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score < 76.8
              AND e.student_id >= 7
        )
      AND MONTH(s.enrolled_at) IN (1, 3)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY TO_DATE(enrolled_at), enrolled_at_month) AS rows_in_enrolled_at_to_date_enrolled_at_month,
        ROUND(AVG(enrolled_at_month) OVER w_enrolled_at_to_date_enrolled, 2) AS enrolled_at_to_date_enrolled_at_month_avg_enroll
    FROM filtered
    WINDOW w_enrolled_at_to_date_enrolled AS (PARTITION BY TO_DATE(enrolled_at), enrolled_at_month)
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
        s.enrolled_at_month,
        s.term_ref,
        s.rows_in_enrolled_at_to_date_enrolled_at_month,
        st.is_international AS student_is_international
    FROM with_trends s
    JOIN students st ON st.id = s.student_id AND st.major IS NOT NULL
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at_month,
        student_is_international,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM with_details_student
    WHERE student_id IS NOT NULL
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at_month + score AS enrolled_at_month_plus_score,
        ROUND(score / NULLIF(enrolled_at_month, 0), 3) AS score_per_enrolled_at_month
    FROM labeled
    WHERE score <= 56.1
)
SELECT
    id,
    student_id,
    score,
    enrolled_at_month_plus_score,
    score_per_enrolled_at_month,
    ABS(score - 38.3) AS score_distance
FROM annotated
WHERE score BETWEEN 23.7 AND 57.8
ORDER BY id DESC, enrolled_at_month_plus_score DESC NULLS FIRST
