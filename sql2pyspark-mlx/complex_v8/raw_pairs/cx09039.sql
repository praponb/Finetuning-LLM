WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE score > 20.2
),
combined_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.score_bucket,
        c.course_code,
        st.student_name,
        st.gpa AS student_gpa
    FROM base_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER w_course_id_by_score_bucket AS score_bucket_decile,
        LAST_VALUE(student_gpa) IGNORE NULLS OVER (ORDER BY student_gpa NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_student_gpa,
        MAX(student_gpa) OVER (PARTITION BY grade_status) AS grade_status_max_student_gpa,
        MIN(score) OVER (PARTITION BY grade_status, course_id ORDER BY score_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        NTH_VALUE(student_gpa, 2) OVER (ORDER BY score_bucket ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_gpa,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY score NULLS LAST) AS score_dense_rank,
        NTH_VALUE(score_bucket, 2) OVER (PARTITION BY grade_status, course_id ORDER BY score_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_bucket,
        student_gpa - LAG(student_gpa) OVER (ORDER BY student_gpa NULLS LAST, id) AS student_gpa_change,
        LEAD(score_bucket, 3, 0) OVER (ORDER BY student_gpa NULLS LAST, id) AS next3_score_bucket
    FROM combined_details_course
    WINDOW w_course_id_by_score_bucket AS (PARTITION BY course_id ORDER BY score_bucket, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        score_bucket,
        course_code,
        score_bucket_decile,
        grade_status_max_student_gpa,
        min_score_so_far,
        score_dense_rank,
        student_gpa_change,
        ABS(score_bucket - 65) AS score_bucket_distance,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        INITCAP(TRIM(course_code)) AS course_code_clean
    FROM sequenced
)
SELECT
    id,
    score,
    grade_status,
    score_bucket,
    course_code,
    grade_status_max_student_gpa,
    min_score_so_far,
    score_dense_rank,
    score_bucket_distance,
    grade_status_or_unknown,
    course_code_clean
FROM tagged
WHERE student_id <= 10
ORDER BY id DESC, score_bucket
LIMIT 5
