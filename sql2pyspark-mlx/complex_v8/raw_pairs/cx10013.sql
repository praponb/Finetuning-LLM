WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        HOUR(enrolled_at) AS enrolled_at_hour
    FROM enrollments
    WHERE score >= 32.7
      AND course_id <= 8
),
with_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_hour,
        c.level AS course_level,
        st.gpa AS student_gpa
    FROM scoped_enrollments s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.level IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        enrolled_at_hour,
        course_level,
        SIZE(COLLECT_SET(course_level) OVER (PARTITION BY grade_status)) AS distinct_course_level_in_grade_status,
        MIN(student_gpa) OVER (PARTITION BY student_id) AS student_id_min_student_gpa,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at_hour DESC), 3) AS enrolled_at_hour_cume_dist,
        ROUND(enrolled_at_hour - AVG(enrolled_at_hour) OVER w_student_id, 2) AS enrolled_at_hour_vs_student_id_avg,
        ROUND(PERCENT_RANK() OVER w_all_by_student_gpa, 3) AS student_gpa_percent_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        RANK() OVER (PARTITION BY course_id, course_level ORDER BY score NULLS LAST) AS score_rank,
        RANK() OVER (PARTITION BY course_id, course_level ORDER BY score NULLS LAST, id) AS score_rank_2,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id, course_level ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        MAX(score) OVER (PARTITION BY course_id, course_level ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY student_gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2,
        MIN(enrolled_at_hour) OVER (PARTITION BY enrolled_at_hour ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_enrolled_at_hour_so_far
    FROM with_details_course
    WINDOW w_student_id AS (PARTITION BY student_id), w_all_by_student_gpa AS (ORDER BY student_gpa DESC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        course_id,
        score,
        grade_status,
        course_level,
        student_id_min_student_gpa,
        student_gpa_percent_rank,
        score_rank_2,
        max_score_so_far,
        second_enrolled_at_2,
        ROUND(student_gpa_percent_rank * 1.5, 2) AS student_gpa_percent_rank_adj,
        INITCAP(TRIM(grade_status)) AS grade_status_clean
    FROM sequenced
)
SELECT
    id,
    course_id,
    score,
    grade_status,
    course_level,
    student_id_min_student_gpa,
    score_rank_2,
    max_score_so_far,
    second_enrolled_at_2,
    grade_status_clean,
    LOWER(TRIM(grade_status)) AS grade_status_clean_2
FROM labeled
WHERE max_score_so_far < 600.0
ORDER BY second_enrolled_at_2 DESC
