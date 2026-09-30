WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref,
        ABS(score - 44.4) AS score_distance,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek
    FROM enrollments
    WHERE course_id <= 9
      AND enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS max_enrollment_score,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at
    FROM valid_enrollments s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        term,
        score,
        score_distance,
        enrolled_at_dayofweek,
        max_enrollment_score,
        avg_enrollment_score,
        last_enrollment_enrolled_at,
        MAX(max_enrollment_score) OVER (PARTITION BY course_id, student_id) AS course_id_student_id_max_max_enrollment_score,
        ROW_NUMBER() OVER (PARTITION BY enrolled_at_dayofweek ORDER BY avg_enrollment_score DESC, id) AS rn
    FROM summarized_rows
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score
    FROM sequenced s
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM profiled s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
        )
),
joined AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.term,
        s.score,
        s.score_distance,
        s.max_enrollment_score,
        s.avg_enrollment_score,
        s.last_enrollment_enrolled_at,
        s.rn,
        s.total_enrollment_score,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international
    FROM filtered s
    JOIN students st ON st.id = s.student_id
    WHERE st.enrolled_on >= DATE '2024-10-09'
)
SELECT
    id,
    score_distance,
    avg_enrollment_score,
    rn,
    total_enrollment_score,
    student_enrolled_on,
    student_cohort,
    student_is_international
FROM joined
ORDER BY student_cohort DESC, score_distance DESC
