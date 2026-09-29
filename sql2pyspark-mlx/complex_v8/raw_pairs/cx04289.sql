WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        ABS(score - 56.1) AS score_distance
    FROM enrollments
    WHERE student_id IS NOT NULL
),
latest_per_key AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        term_prefix,
        days_since_enrolled_at,
        score_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY days_since_enrolled_at DESC, id DESC) AS rn
        FROM cleaned_enrollments
    ) ranked
    WHERE rn = 1
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(days_since_enrolled_at / 100) * 100 AS days_since_enrolled_at_bucket,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label,
        LENGTH(TRIM(term_prefix)) AS term_prefix_len
    FROM latest_per_key
    WHERE term IS NOT NULL
),
with_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.term_prefix,
        s.days_since_enrolled_at,
        s.score_distance,
        s.days_since_enrolled_at_bucket,
        s.enrolled_at_label,
        s.term_prefix_len,
        st.gpa AS student_gpa,
        st.major AS student_major,
        st.student_name,
        c.course_code
    FROM derived s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
)
SELECT
    student_id,
    student_major,
    course_id,
    SUM(days_since_enrolled_at_bucket) FILTER (WHERE UPPER(TRIM(student_name)) LIKE 'J%') AS days_since_enrolled_at_bucket_upper_trim_student
FROM with_details_student
GROUP BY ROLLUP(student_id, student_major, course_id)
ORDER BY student_id NULLS LAST, student_major NULLS LAST, course_id NULLS LAST
