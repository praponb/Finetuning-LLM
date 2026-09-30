WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        HOUR(enrolled_at) AS enrolled_at_hour,
        enrolled_at + INTERVAL 2 HOURS AS enrolled_at_plus_2h
    FROM enrollments
    WHERE grade_status <> 'FAIL'
      AND course_id IS NOT NULL
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id <= 15
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        grade_status,
        enrolled_at,
        enrolled_at_plus_2h,
        LEAD(enrolled_at) OVER (PARTITION BY grade_status, TO_DATE(enrolled_at_plus_2h) ORDER BY enrolled_at_hour DESC, id) AS next_enrolled_at,
        MAX(enrolled_at_hour) OVER w_term AS term_max_enrolled_at_hour,
        LAST_VALUE(enrolled_at_hour) OVER (PARTITION BY grade_status, TO_DATE(enrolled_at_plus_2h) ORDER BY enrolled_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at_hour,
        LAG(enrolled_at) OVER w_grade_status_enrolled_at_plu AS prev_enrolled_at
    FROM without_matches
    WINDOW w_term AS (PARTITION BY term), w_grade_status_enrolled_at_plu AS (PARTITION BY grade_status, TO_DATE(enrolled_at_plus_2h) ORDER BY enrolled_at_hour DESC, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        grade_status,
        enrolled_at_plus_2h,
        next_enrolled_at,
        term_max_enrolled_at_hour,
        prev_enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        CASE WHEN last_enrolled_at_hour >= 13 THEN 'A' WHEN last_enrolled_at_hour >= 10 THEN 'B' ELSE 'C' END AS last_enrolled_at_hour_tier
    FROM metrics
    WHERE HOUR(prev_enrolled_at) BETWEEN 9 AND 15
),
joined AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.grade_status,
        s.enrolled_at_plus_2h,
        s.next_enrolled_at,
        s.term_max_enrolled_at_hour,
        s.prev_enrolled_at,
        s.grade_status_slug,
        s.last_enrolled_at_hour_tier,
        st.enrolled_on AS student_enrolled_on
    FROM tagged s
    JOIN students st ON st.id = s.student_id
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.grade_status,
        s.grade_status_slug,
        s.last_enrolled_at_hour_tier,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at,
        e.course_id AS enrollment_course_id
    FROM joined s
    LEFT JOIN enrollments e ON e.student_id = s.student_id AND e.term = '2024T1'
)
SELECT
    id,
    student_id,
    grade_status,
    grade_status_slug,
    enrollment_enrolled_at
FROM detail_rows
ORDER BY enrollment_enrolled_at DESC, id DESC
