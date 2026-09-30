WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 7 AND 11
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        term,
        session_no,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY term, session_no
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        event_count,
        SUM(session_no) FILTER (WHERE term IN ('2025T1', '2024T2')) AS session_no_term_in_2025t1_2024t2,
        SUM(CASE WHEN term IN ('2024T2', '2024T1', '2025T1') THEN 1 ELSE 0 END) AS n_term_in_2024t2_2024t1_2025t1,
        ROUND(SUM(event_count) / NULLIF(SUM(session_no), 0), 3) AS event_count_to_session_no_ratio,
        COUNT(DISTINCT term) AS distinct_terms
    FROM per_session
    WHERE duration_min >= 108.0
    GROUP BY event_count
),
windowed AS (
    -- Previous / next values per key
    SELECT
        event_count,
        n_term_in_2024t2_2024t1_2025t1,
        event_count_to_session_no_ratio,
        SUM(distinct_terms) OVER w_distinct_terms AS distinct_terms_sum_distinct_terms,
        ROUND(event_count / NULLIF(SUM(event_count) OVER (PARTITION BY distinct_terms), 0), 4) AS event_count_share_of_distinct_terms,
        ROW_NUMBER() OVER (PARTITION BY distinct_terms, event_count ORDER BY n_term_in_2024t2_2024t1_2025t1 DESC, event_count) AS rn
    FROM per_group
    WINDOW w_distinct_terms AS (PARTITION BY distinct_terms)
)
-- Native SQL UNPIVOT (wide to long)
SELECT event_count, measure, value
FROM (
    SELECT event_count, event_count_share_of_distinct_terms, event_count_to_session_no_ratio
    FROM windowed
) src
UNPIVOT (
    value FOR measure IN (event_count_share_of_distinct_terms AS m_event_count_share_of_distinct_terms, event_count_to_session_no_ratio AS m_event_count_to_session_no_ratio)
) unp
ORDER BY event_count, measure
