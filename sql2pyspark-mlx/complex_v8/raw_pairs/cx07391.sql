WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE (HOUR(enrolled_at) BETWEEN 6 AND 12 OR score < 78.8)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(score, 3) OVER w_all_by_score_filled AS prev3_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status, course_id ORDER BY enrolled_at DESC), 3) AS enrolled_at_percent_rank,
        ROUND(score - AVG(score) OVER w_enrolled_at_year, 2) AS score_vs_enrolled_at_year_avg,
        LAG(score_filled) OVER (ORDER BY score_filled, id) AS prev_score_filled,
        LEAD(enrolled_at) OVER w_grade_status_course_id_by_en_2 AS next_enrolled_at,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY YEAR(enrolled_at))) AS distinct_student_id_in_enrolled_at_year,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY score_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        LAST_VALUE(score) OVER (PARTITION BY grade_status, course_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score
    FROM prepared_enrollments
    WINDOW w_all_by_score_filled AS (ORDER BY score_filled, id), w_enrolled_at_year AS (PARTITION BY YEAR(enrolled_at)), w_grade_status_course_id_by_en_2 AS (PARTITION BY grade_status, course_id ORDER BY enrolled_at DESC, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ABS(last_score - 38.3) AS last_score_distance,
        FLOOR(prev3_score / 25) * 25 AS prev3_score_bucket
    FROM windowed
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    score_filled,
    prev3_score,
    enrolled_at_percent_rank,
    score_vs_enrolled_at_year_avg,
    prev_score_filled,
    next_enrolled_at,
    distinct_student_id_in_enrolled_at_year,
    last_score,
    last_score_distance
FROM tagged
WHERE score_filled <= 85.0
ORDER BY enrolled_at_percent_rank
