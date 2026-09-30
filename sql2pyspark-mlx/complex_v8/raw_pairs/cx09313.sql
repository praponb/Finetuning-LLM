WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 44.4) AS score_distance
    FROM enrollments
    WHERE student_id <= 17
),
enriched AS (
    -- Label and score rows
    SELECT *, FLOOR(score_distance / 25) * 25 AS score_distance_bucket
    FROM valid
),
joined_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score_distance,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international
    FROM enriched s
    LEFT JOIN students st ON st.id = s.student_id
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM joined_student s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status NOT IN ('PASS', 'INCOMPLETE')
        )
      AND s.student_cohort IS NOT NULL
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(score_distance, student_gpa) AS least_score_distance_student_gpa
    FROM filtered
    WHERE score_distance <= 85.0
)
SELECT
    id,
    student_id,
    course_id,
    score_distance,
    student_gpa,
    student_cohort,
    student_is_international,
    least_score_distance_student_gpa,
    SUBSTRING(TRIM(student_cohort), 1, 2) AS student_cohort_prefix
FROM scored
