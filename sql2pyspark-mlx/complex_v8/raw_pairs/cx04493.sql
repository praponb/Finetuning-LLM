WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 78.8 THEN 'HIGH' WHEN score >= 38.1 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-03-08 14:00:00'
),
with_prev AS (
    -- Previous enrolled_at per score_tier
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY score_tier ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY score_tier ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        score_tier,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY score_tier, session_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(n_events - AVG(n_events) OVER w_session_id, 2) AS n_events_vs_session_id_avg,
        NTH_VALUE(n_events, 2) OVER (PARTITION BY score_tier, YEAR(session_start) ORDER BY session_start, score_tier, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_events
    FROM per_session
    WINDOW w_session_id AS (PARTITION BY session_id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        session_id,
        COUNT(DISTINCT score_tier) AS distinct_score_tiers,
        COUNT(*) AS cnt,
        PERCENTILE_APPROX(duration_min, 0.5) AS median_duration_min,
        ROUND(STDDEV(duration_min), 2) AS stddev_duration_min,
        SUM(duration_min) AS total_duration_min,
        MAX(session_end) AS last_session_end
    FROM windowed
    GROUP BY session_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT session_id, metric_name, metric_value
FROM (
    SELECT session_id, median_duration_min, stddev_duration_min
    FROM grouped
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (median_duration_min AS m_median_duration_min, stddev_duration_min AS m_stddev_duration_min)
) unp
ORDER BY session_id, metric_name
