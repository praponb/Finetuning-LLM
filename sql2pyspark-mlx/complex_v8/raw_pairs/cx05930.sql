WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE course_id <= 6
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM scoped_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND HOUR(e.enrolled_at) BETWEEN 10 AND 12
              AND e.score < 76.8
        )
),
latest_per_key AS (
    -- Keep the earliest row per grade_status, student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        grade_status_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status, student_id ORDER BY grade_status_len, id) AS recency_rank
        FROM with_activity
    ) ranked
    WHERE recency_rank = 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY student_id, grade_status ORDER BY grade_status_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(PERCENT_RANK() OVER w_all_by_score, 3) AS score_percent_rank,
        ROUND(grade_status_len / NULLIF(SUM(grade_status_len) OVER (PARTITION BY student_id, grade_status_len), 0), 4) AS grade_status_len_share_of_student_id_grade_statu,
        LEAD(score) OVER (ORDER BY score, id) AS next_score,
        MIN(score) OVER (PARTITION BY student_id, grade_status_len) AS student_id_grade_status_len_min_score
    FROM latest_per_key
    WINDOW w_all_by_score AS (ORDER BY score, id)
)
SELECT *
FROM (
    SELECT student_id, second_enrolled_at, grade_status, score
    FROM with_trends
) src
PIVOT (
    MIN(score) AS min_v, COUNT(score) AS n
    FOR grade_status IN ('INCOMPLETE', 'FAIL', 'PASS', 'WITHDRAWN')
)
ORDER BY student_id, second_enrolled_at
