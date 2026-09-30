WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE score BETWEEN 8.8 AND 73.0
      AND student_id <= 7
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        ROUND(AVG(score) OVER (PARTITION BY student_id), 2) AS student_id_avg_score,
        ROW_NUMBER() OVER (PARTITION BY course_id, term ORDER BY score DESC, id) AS rn,
        COUNT(*) OVER (PARTITION BY student_id, term) AS rows_in_student_id_term,
        LEAD(score, 2) OVER w_student_id_by_enrolled_at AS next2_score,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at_2,
        RANK() OVER (PARTITION BY student_id ORDER BY enrolled_at) AS enrolled_at_rank,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY course_id, term ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        LEAD(enrolled_at) OVER w_student_id_by_enrolled_at AS next_enrolled_at
    FROM prepared_enrollments
    WINDOW w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at, id)
)
-- Final projection
SELECT
    id,
    course_id,
    score,
    rn,
    rows_in_student_id_term,
    first_known_enrolled_at,
    enrolled_at_rank,
    last_known_score,
    enrolled_at_percent_rank,
    last_known_enrolled_at,
    next_enrolled_at,
    HOUR(next_enrolled_at) AS next_enrolled_at_hour
FROM ranked
ORDER BY enrolled_at_rank NULLS LAST, id DESC
LIMIT 20
