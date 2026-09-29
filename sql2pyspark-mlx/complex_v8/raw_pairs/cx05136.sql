WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix,
        ABS(score - 38.3) AS score_distance
    FROM enrollments
    WHERE term = '2025T2'
),
term_stats AS (
    -- Avg score_distance per term
    SELECT term, AVG(score_distance) AS avg_score_distance
    FROM filtered_enrollments
    GROUP BY term
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_score_distance,
        ROUND(s.score_distance - g.avg_score_distance, 2) AS score_distance_minus_avg
    FROM filtered_enrollments s
    JOIN term_stats g ON g.term = s.term
    WHERE s.score_distance > g.avg_score_distance
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', grade_status_prefix, term) AS grade_status_prefix_term_label,
        ROUND(score_distance_minus_avg * 0.25, 2) AS score_distance_minus_avg_adj
    FROM above_average
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.grade_status_prefix,
        s.score_distance,
        s.avg_score_distance,
        s.score_distance_minus_avg,
        c.level AS course_level,
        c.dept AS course_dept,
        st.gpa AS student_gpa
    FROM scored s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at,
        grade_status_prefix,
        score_distance,
        avg_score_distance,
        score_distance_minus_avg,
        course_level,
        student_gpa,
        ROUND(score_distance - AVG(score_distance) OVER w_course_dept_course_id, 2) AS score_distance_vs_course_dept_course_id_avg,
        COUNT(*) OVER (PARTITION BY course_dept, course_id) AS rows_in_course_dept_course_id,
        ROUND(avg_score_distance - AVG(avg_score_distance) OVER w_course_dept_course_id, 2) AS avg_score_distance_vs_course_dept_course_id_avg,
        ROUND(CUME_DIST() OVER w_student_id_term_by_score_dis, 3) AS score_distance_cume_dist
    FROM with_details_course
    WINDOW w_course_dept_course_id AS (PARTITION BY course_dept, course_id), w_student_id_term_by_score_dis AS (PARTITION BY student_id, term ORDER BY score_distance DESC)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        student_id,
        SUM(score_distance_cume_dist) AS total_score_distance_cume_dist,
        ARRAY_SORT(COLLECT_SET(course_level)) AS course_level_list,
        ROUND(SUM(avg_score_distance), 2) AS total_avg_score_distance,
        MAX(enrolled_at) AS last_enrolled_at
    FROM with_trends
    WHERE score_distance_cume_dist < 0.7
    GROUP BY student_id
    HAVING MAX(score_distance_vs_course_dept_course_id_avg) > -30.0
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score
    FROM summary s
)
SELECT
    student_id,
    total_score_distance_cume_dist,
    course_level_list,
    last_enrolled_at,
    total_enrollment_score,
    enrollment_count,
    avg_enrollment_score
FROM summarized_rows
ORDER BY avg_enrollment_score
