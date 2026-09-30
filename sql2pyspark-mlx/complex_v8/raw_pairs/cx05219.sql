WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 2)
),
joined AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        st.gpa AS student_gpa,
        st.student_name
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id
    WHERE UPPER(TRIM(st.student_name)) LIKE 'B%'
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status IS NOT NULL
        )
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM filtered
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        course_id,
        term,
        score,
        enrolled_at,
        student_gpa,
        student_name,
        enrolled_at_week,
        SUM(student_gpa) OVER (ORDER BY enrolled_at_week DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS student_gpa_sum_1_prior,
        ROUND(PERCENT_RANK() OVER w_course_id_enrolled_at_month, 3) AS student_gpa_percent_rank
    FROM enriched
    WINDOW w_course_id_enrolled_at_month AS (PARTITION BY course_id, MONTH(enrolled_at) ORDER BY student_gpa ASC)
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM with_trends s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
profiled_2 AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.course_id = s.course_id) AS avg_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM profiled s
)
SELECT id, field, reading
FROM (
    SELECT id, student_gpa_percent_rank, student_gpa_sum_1_prior
    FROM profiled_2
) src
UNPIVOT (
    reading FOR field IN (student_gpa_percent_rank, student_gpa_sum_1_prior)
) unp
ORDER BY id, field
