WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT *
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-18 08:00:00'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_id, course_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        SUM(score) OVER (PARTITION BY grade_status ORDER BY score ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_sum_6_prior,
        COUNT(*) OVER w_grade_status AS rows_in_grade_status,
        ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY score DESC, id) AS score_seq,
        ROUND(AVG(score) OVER (PARTITION BY student_id, course_id ORDER BY score DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        ROUND(AVG(score) OVER (PARTITION BY grade_status), 2) AS grade_status_avg_score
    FROM recent_enrollments
    WINDOW w_grade_status AS (PARTITION BY grade_status)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT course_id, last_known_enrolled_at, grade_status, score_sum_6_prior
    FROM ranked
) src
PIVOT (
    AVG(score_sum_6_prior) AS avg_v, COUNT(score_sum_6_prior) AS n
    FOR grade_status IN ('FAIL', 'WITHDRAWN', 'PASS', 'INCOMPLETE')
)
