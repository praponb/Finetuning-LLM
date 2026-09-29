WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM enrollments
    WHERE course_id >= 6
      AND enrolled_at < TIMESTAMP '2025-02-18 11:00:00'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(score, 2) OVER (ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        LEAD(score) OVER (PARTITION BY grade_status ORDER BY score, id) AS next_score,
        score - LAG(score) OVER (PARTITION BY grade_status ORDER BY score, id) AS score_change,
        ROUND(AVG(score) OVER (PARTITION BY student_id), 2) AS student_id_avg_score,
        RANK() OVER (PARTITION BY term ORDER BY score NULLS LAST, id) AS score_rank,
        LAG(score, 3) OVER (PARTITION BY term ORDER BY score NULLS LAST, id) AS prev3_score,
        MIN(score) OVER (PARTITION BY student_id) AS student_id_min_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id), 2) AS score_vs_student_id_avg,
        score - LAG(score) OVER (PARTITION BY term ORDER BY score NULLS LAST, id) AS score_change_2,
        COUNT(*) OVER w_student_id AS rows_in_student_id,
        LAST_VALUE(score) OVER (PARTITION BY term ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score
    FROM filtered_enrollments
    WINDOW w_student_id AS (PARTITION BY student_id)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    second_score,
    next_score,
    score_rank,
    prev3_score,
    score_share_of_student_id,
    score_vs_student_id_avg,
    score_change_2,
    last_score,
    COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
FROM windowed
ORDER BY student_id, id
LIMIT 5
