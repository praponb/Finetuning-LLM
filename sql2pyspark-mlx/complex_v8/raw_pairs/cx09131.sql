WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE term IN ('2024T1', '2024T2')
      AND score > 17.4
),
labeled AS (
    -- Add calculated columns
    SELECT *, ABS(score - 38.5) AS score_distance
    FROM base_enrollments
    WHERE course_id IS NOT NULL
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY grade_status, term) AS rows_in_grade_status_term,
        LEAD(score_distance) OVER (PARTITION BY course_id, term ORDER BY score, id) AS next_score_distance,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER w_grade_status_term, 0), 4) AS score_distance_share_of_grade_status_term,
        ROUND(PERCENT_RANK() OVER (ORDER BY score_distance), 3) AS score_distance_percent_rank,
        score - LAG(score) OVER (ORDER BY score_distance, id) AS score_change,
        RANK() OVER (PARTITION BY course_id, term ORDER BY score) AS score_rank
    FROM qualified
    WINDOW w_grade_status_term AS (PARTITION BY grade_status, term)
)
SELECT *
FROM (
    SELECT student_id, enrolled_at, grade_status, score
    FROM sequenced
) src
PIVOT (
    SUM(score) AS amount, MAX(score) AS max_v
    FOR grade_status IN ('PASS', 'WITHDRAWN', 'INCOMPLETE')
)
