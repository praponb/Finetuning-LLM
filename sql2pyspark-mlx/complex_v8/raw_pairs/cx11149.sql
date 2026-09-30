WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 59.7) AS score_distance,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE grade_status IN ('PASS', 'INCOMPLETE', 'FAIL')
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        score_distance,
        score_bucket,
        FLOOR(score_distance / 25) * 25 AS score_distance_bucket,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        HOUR(enrolled_at) AS enrolled_at_hour
    FROM prepared
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        enrolled_at_hour,
        grade_status,
        ROUND(SUM(score_distance) / NULLIF(SUM(score_distance_bucket), 0), 3) AS score_distance_to_score_distance_bucket_ratio,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        MIN(enrolled_at_hour) AS min_enrolled_at_hour
    FROM tagged
    GROUP BY enrolled_at_hour, grade_status
),
per_label AS (
    -- One row per term_list_item
    SELECT
        s.enrolled_at_hour,
        s.grade_status,
        s.score_distance_to_score_distance_bucket_ratio,
        s.min_enrolled_at_hour,
        t.term_list_item
    FROM rollup_base s
    LATERAL VIEW OUTER EXPLODE(s.term_list) t AS term_list_item
)
SELECT
    enrolled_at_hour,
    grade_status,
    score_distance_to_score_distance_bucket_ratio,
    term_list_item,
    COALESCE(enrolled_at_hour, 0) AS enrolled_at_hour_filled,
    ABS(min_enrolled_at_hour - 10) AS min_enrolled_at_hour_distance
FROM per_label
ORDER BY grade_status DESC, term_list_item DESC
