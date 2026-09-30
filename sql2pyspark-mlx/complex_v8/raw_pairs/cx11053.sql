WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 57.6) AS score_distance,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(score) OVER (ORDER BY score_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        COUNT(*) OVER (PARTITION BY student_id, grade_status) AS rows_in_student_id_grade_status,
        MIN(score_distance) OVER (ORDER BY score_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_distance_so_far,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id, grade_status), 2) AS score_vs_student_id_grade_status_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY score_distance), 3) AS score_distance_percent_rank,
        MIN(score_distance) OVER (PARTITION BY student_id, grade_status) AS student_id_grade_status_min_score_distance,
        MIN(score) OVER (PARTITION BY student_id, grade_status) AS student_id_grade_status_min_score,
        ROUND(CUME_DIST() OVER (ORDER BY score_distance, id), 3) AS score_distance_cume_dist
    FROM recent_enrollments
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        score_distance - LAG(score_distance) OVER (ORDER BY student_id_grade_status_min_score NULLS LAST, id) AS score_distance_change,
        min_score_distance_so_far - LAG(min_score_distance_so_far) OVER (PARTITION BY grade_status ORDER BY student_id_grade_status_min_score DESC, id) AS min_score_distance_so_far_change,
        FIRST_VALUE(min_score_so_far) OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_min_score_so_far,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER (PARTITION BY student_id), 0), 4) AS score_distance_share_of_student_id,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY grade_status)) AS distinct_student_id_in_grade_status
    FROM sequenced
)
SELECT
    id,
    student_id,
    score,
    grade_status,
    enrolled_at,
    score_distance,
    grade_status_prefix,
    min_score_so_far,
    rows_in_student_id_grade_status,
    min_score_distance_so_far,
    score_vs_student_id_grade_status_avg,
    score_distance_percent_rank,
    student_id_grade_status_min_score_distance,
    student_id_grade_status_min_score,
    score_distance_change,
    min_score_distance_so_far_change,
    score_distance_share_of_student_id,
    distinct_student_id_in_grade_status,
    score + score_distance_change AS score_plus_score_distance_change,
    LENGTH(TRIM(grade_status)) AS grade_status_len
FROM with_windows
ORDER BY grade_status DESC, student_id_grade_status_min_score NULLS LAST
