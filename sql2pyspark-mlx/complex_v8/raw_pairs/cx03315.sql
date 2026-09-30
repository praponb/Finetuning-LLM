WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CASE WHEN score >= 57.6 THEN 'A' WHEN score >= 26.2 THEN 'B' ELSE 'C' END AS score_tier,
        DATE_TRUNC('HOUR', enrolled_at) AS enrolled_at_hour
    FROM enrollments
    WHERE course_id >= 2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term,
        score,
        score_tier,
        enrolled_at_hour,
        ROUND((UNIX_TIMESTAMP(enrolled_at_hour) - UNIX_TIMESTAMP(LAG(enrolled_at_hour) OVER (PARTITION BY course_id ORDER BY enrolled_at_hour NULLS LAST, id))) / 60, 1) AS minutes_since_prev_enrolled_at_hour,
        FIRST_VALUE(enrolled_at_hour) IGNORE NULLS OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at_hour,
        SUM(score) OVER (PARTITION BY term) AS term_sum_score,
        COUNT(*) OVER w_term AS rows_in_term
    FROM recent_enrollments
    WINDOW w_term AS (PARTITION BY term)
)
SELECT id, field, reading
FROM (
    SELECT id, minutes_since_prev_enrolled_at_hour, score, term_sum_score
    FROM with_trends
) src
UNPIVOT (
    reading FOR field IN (minutes_since_prev_enrolled_at_hour, score, term_sum_score)
) unp
