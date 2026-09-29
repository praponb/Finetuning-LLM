WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE course_id IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.grade_status_or_unknown,
        c.level AS course_level,
        c.course_code,
        c.dept AS course_dept
    FROM recent s
    LEFT JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        course_level,
        course_code,
        course_dept,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status_or_unknown, grade_status ORDER BY score DESC), 3) AS score_cume_dist,
        LEAD(score, 1, 0) OVER (PARTITION BY grade_status, course_id ORDER BY score ASC, id) AS next_score,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status_or_unknown), 2) AS score_vs_grade_status_or_unknown_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status_or_unknown, course_level ORDER BY score), 3) AS score_cume_dist_2,
        NTH_VALUE(score, 2) OVER (PARTITION BY grade_status_or_unknown, course_level ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        COUNT(*) OVER (PARTITION BY course_id, grade_status) AS rows_in_course_id_grade_status,
        ROW_NUMBER() OVER (PARTITION BY grade_status_or_unknown, grade_status ORDER BY score DESC, id) AS seq_no,
        ROUND(score / NULLIF(SUM(score) OVER w_course_level_grade_status_or, 0), 4) AS score_share_of_course_level_grade_status_or_unkn,
        ROUND(CUME_DIST() OVER w_grade_status_course_id_by_sc_2, 3) AS score_cume_dist_3,
        ROUND(AVG(score) OVER (PARTITION BY grade_status_or_unknown, course_level ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_avg_2,
        SIZE(COLLECT_SET(grade_status_or_unknown) OVER w_course_id_grade_status) AS distinct_grade_status_or_unknown_in_course_id_gr,
        SIZE(COLLECT_SET(course_level) OVER (PARTITION BY course_id, grade_status)) AS distinct_course_level_in_course_id_grade_status,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status, course_id ORDER BY score), 3) AS score_percent_rank,
        score - LAG(score) OVER (PARTITION BY grade_status, course_id ORDER BY score, id) AS score_change,
        RANK() OVER w_student_id_grade_status_by_s AS score_rank,
        ROUND(CUME_DIST() OVER w_all_by_score, 3) AS score_cume_dist_4
    FROM joined
    WINDOW w_course_level_grade_status_or AS (PARTITION BY course_level, grade_status_or_unknown), w_grade_status_course_id_by_sc_2 AS (PARTITION BY grade_status, course_id ORDER BY score), w_course_id_grade_status AS (PARTITION BY course_id, grade_status), w_student_id_grade_status_by_s AS (PARTITION BY student_id, grade_status ORDER BY score), w_all_by_score AS (ORDER BY score, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE seq_no = 1
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        course_code,
        course_dept,
        score_vs_grade_status_or_unknown_avg,
        second_score,
        rows_in_course_id_grade_status,
        score_share_of_course_level_grade_status_or_unkn,
        score_cume_dist_3,
        score_avg_2,
        distinct_course_level_in_course_id_grade_status,
        score_percent_rank,
        score_change,
        score_rank,
        score_cume_dist_4,
        MIN(score) OVER (PARTITION BY grade_status) AS grade_status_min_score,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        NTILE(10) OVER w_grade_status_by_score_avg_2 AS score_avg_2_decile
    FROM leaders
    WINDOW w_grade_status_by_score_avg_2 AS (PARTITION BY grade_status ORDER BY score_avg_2 DESC, id)
)
SELECT
    id,
    course_id,
    course_code,
    course_dept,
    second_score,
    rows_in_course_id_grade_status,
    score_share_of_course_level_grade_status_or_unkn,
    score_cume_dist_3,
    score_avg_2,
    distinct_course_level_in_course_id_grade_status,
    score_percent_rank,
    score_change,
    score_cume_dist_4,
    grade_status_min_score,
    rows_in_grade_status,
    score_avg_2_decile,
    ABS(score_cume_dist_3 - 0.6) AS score_cume_dist_3_distance
FROM sequenced_2
ORDER BY score_cume_dist_4
