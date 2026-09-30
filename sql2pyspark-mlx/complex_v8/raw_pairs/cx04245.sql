WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        YEAR(enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE course_id <= 8
),
joined_student AS (
    -- Join students
    SELECT
        s.*,
        st.is_international AS student_is_international,
        st.major AS student_major
    FROM valid_enrollments s
    JOIN students st ON st.id = s.student_id
    WHERE st.major NOT IN ('Art', 'Biz')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(enrolled_at_year / NULLIF(SUM(enrolled_at_year) OVER (PARTITION BY student_is_international, enrolled_at_year), 0), 4) AS enrolled_at_year_share_of_student_is_internation,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY score ASC, id), 3) AS score_cume_dist,
        NTILE(10) OVER (ORDER BY enrolled_at, id) AS enrolled_at_decile,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY student_is_international, enrolled_at_year)) AS distinct_student_id_in_student_is_international_
    FROM joined_student
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS(': ', grade_status, student_major) AS grade_status_student_major_label,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref
    FROM sequenced
),
enriched AS (
    -- Join courses
    SELECT s.*, c.level AS course_level
    FROM derived s
    JOIN courses c ON c.id = s.course_id AND LOWER(c.course_code) LIKE '%ma%'
    WHERE c.level IS NOT NULL
),
joined_student_2 AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.enrolled_at_year,
        s.student_is_international,
        s.student_major,
        s.enrolled_at_year_share_of_student_is_internation,
        s.score_cume_dist,
        s.enrolled_at_decile,
        s.distinct_student_id_in_student_is_international_,
        s.grade_status_ref,
        s.course_level,
        st.enrolled_on AS student_enrolled_on,
        st.major AS student_major_2,
        st.student_name,
        c.dept AS course_dept
    FROM enriched s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        YEAR(student_enrolled_on) AS student_enrolled_on_year,
        GREATEST(enrolled_at_year, enrolled_at_decile) AS greatest_enrolled_at_year_enrolled_at_decile,
        GREATEST(enrolled_at_decile, score_cume_dist) AS greatest_enrolled_at_decile_score_cume_dist
    FROM joined_student_2
)
SELECT
    id,
    student_id,
    course_id,
    enrolled_at_year,
    student_major,
    score_cume_dist,
    distinct_student_id_in_student_is_international_,
    grade_status_ref,
    course_level,
    student_enrolled_on,
    student_major_2,
    student_name,
    course_dept,
    student_enrolled_on_year,
    greatest_enrolled_at_year_enrolled_at_decile,
    greatest_enrolled_at_decile_score_cume_dist,
    TRUNC(student_enrolled_on, 'MM') AS student_enrolled_on_month_start,
    ABS(score_cume_dist - 0.6) AS score_cume_dist_distance
FROM annotated
ORDER BY distinct_student_id_in_student_is_international_
