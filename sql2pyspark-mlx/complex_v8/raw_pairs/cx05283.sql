WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 38.1) AS score_distance
    FROM enrollments
    WHERE student_id >= 4
),
deduped AS (
    -- Keep the latest row per student_id, term
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, term ORDER BY score_distance DESC, id DESC) AS recency_rank
        FROM recent_enrollments
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        score_distance,
        DENSE_RANK() OVER w_grade_status_student_id_by_s AS score_distance_dense_rank,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        score - LAG(score) OVER (PARTITION BY grade_status ORDER BY score DESC, id) AS score_change,
        LAG(score, 3) OVER (PARTITION BY grade_status ORDER BY score DESC, id) AS prev3_score
    FROM deduped
    WINDOW w_grade_status_student_id_by_s AS (PARTITION BY grade_status, student_id ORDER BY score_distance ASC, id)
)
SELECT
    grade_status,
    term,
    GROUPING_ID() AS gid,
    MAX(rows_in_student_id) AS max_rows_in_student_id,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
    ROUND(SUM(score_distance), 2) AS total_score_distance
FROM windowed
GROUP BY GROUPING SETS ((grade_status, term), (grade_status), (term), ())
ORDER BY grade_status NULLS LAST, term NULLS LAST
