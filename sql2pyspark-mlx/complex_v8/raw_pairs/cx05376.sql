WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix,
        SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE course_id >= 5
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.term_prefix,
        s.grade_status_prefix,
        s.term_slug,
        st.major AS student_major,
        st.student_name,
        st.enrolled_on AS student_enrolled_on
    FROM scoped_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(score / NULLIF(SUM(score) OVER w_grade_status_student_major, 0), 4) AS score_share_of_grade_status_student_major,
        MAX(score) OVER (PARTITION BY grade_status, student_major) AS grade_status_student_major_max_score,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, student_major), 2) AS grade_status_student_major_avg_score,
        ROUND(score - AVG(score) OVER w_grade_status_student_major, 2) AS score_vs_grade_status_student_major_avg
    FROM joined
    WINDOW w_grade_status_student_major AS (PARTITION BY grade_status, student_major)
)
SELECT
    student_major,
    grade_status,
    course_id,
    PERCENTILE_APPROX(grade_status_student_major_avg_score, 0.5) AS median_grade_status_student_major_avg_score,
    ROUND(100 * COUNT(CASE WHEN grade_status IN ('WITHDRAWN', 'INCOMPLETE', 'FAIL') THEN 1 END) / COUNT(*), 1) AS pct_grade_status_in_withdrawn_in,
    ROUND(SUM(grade_status_student_major_avg_score) / NULLIF(SUM(score_share_of_grade_status_student_major), 0), 3) AS grade_status_student_major_avg_score_to_score_sh
FROM ranked
GROUP BY CUBE(student_major, grade_status, course_id)
ORDER BY student_major NULLS LAST, grade_status NULLS LAST, course_id NULLS LAST
