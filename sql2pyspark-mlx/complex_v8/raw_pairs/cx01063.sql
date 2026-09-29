WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM enrollments
    WHERE course_id <= 11
),
combined_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.enrolled_at_label,
        st.is_international AS student_is_international,
        c.course_code
    FROM recent s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
    WHERE NOT st.is_international
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(score) OVER (PARTITION BY course_id ORDER BY score NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_max_3_prior,
        ROUND(score - AVG(score) OVER (PARTITION BY student_is_international, YEAR(enrolled_at)), 2) AS score_vs_student_is_international_enrolled_at_ye,
        LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY score NULLS LAST, id) AS prev_enrolled_at,
        LEAD(enrolled_at, 3) OVER w_all_by_score AS next3_enrolled_at,
        COUNT(*) OVER (PARTITION BY student_is_international, YEAR(enrolled_at)) AS rows_in_student_is_international_enrolled_at_yea,
        MAX(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_max_3,
        ROW_NUMBER() OVER (ORDER BY enrolled_at, id) AS enrolled_at_seq
    FROM combined_details_student
    WINDOW w_all_by_score AS (ORDER BY score DESC, id)
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    enrolled_at,
    enrolled_at_label,
    course_code,
    score_vs_student_is_international_enrolled_at_ye,
    prev_enrolled_at,
    rows_in_student_is_international_enrolled_at_yea,
    score_max_3,
    enrolled_at_seq,
    DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
    ROUND((UNIX_TIMESTAMP(prev_enrolled_at) - UNIX_TIMESTAMP(enrolled_at)) / 3600, 1) AS hours_enrolled_at_to_prev_enrolled_at
FROM windowed
WHERE score_max_3 < 270.0
ORDER BY rows_in_student_is_international_enrolled_at_yea, hours_enrolled_at_to_prev_enrolled_at DESC
