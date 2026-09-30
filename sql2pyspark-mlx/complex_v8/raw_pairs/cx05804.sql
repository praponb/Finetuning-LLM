WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM enrollments
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(score) OVER (PARTITION BY term ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        ROUND(PERCENT_RANK() OVER w_term_by_score, 3) AS score_percent_rank,
        NTILE(3) OVER (PARTITION BY term ORDER BY score, id) AS score_tercile,
        ROUND(score_filled - AVG(score_filled) OVER w_course_id, 2) AS score_filled_vs_course_id_avg,
        ROUND(AVG(score) OVER (PARTITION BY term ORDER BY score, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        ROW_NUMBER() OVER (PARTITION BY term ORDER BY score, id) AS score_seq
    FROM filtered
    WINDOW w_term_by_score AS (PARTITION BY term ORDER BY score, id), w_course_id AS (PARTITION BY course_id)
),
combined_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.score_filled,
        s.term_prefix,
        s.last_score,
        s.score_tercile,
        s.score_filled_vs_course_id_avg,
        s.score_avg_7,
        st.major AS student_major,
        st.is_international AS student_is_international
    FROM sequenced s
    LEFT JOIN students st ON st.id = s.student_id
),
deduped AS (
    -- Keep the earliest row per term, student_major
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        score_filled,
        term_prefix,
        last_score,
        score_tercile,
        score_filled_vs_course_id_avg,
        score_avg_7,
        student_major,
        student_is_international
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, student_major ORDER BY score_filled_vs_course_id_avg, id) AS rn
        FROM combined_details_student
    ) ranked
    WHERE rn = 1
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score_filled,
        s.term_prefix,
        s.score_filled_vs_course_id_avg,
        s.student_is_international,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id
    FROM deduped s
    JOIN enrollments e ON e.course_id = s.course_id
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score_filled,
    term_prefix,
    score_filled_vs_course_id_avg,
    student_is_international,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_score,
    enrollment_term,
    LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
FROM exploded_detail
WHERE enrollment_term IN ('2025T2', '2024T1')
ORDER BY enrollment_id NULLS LAST
