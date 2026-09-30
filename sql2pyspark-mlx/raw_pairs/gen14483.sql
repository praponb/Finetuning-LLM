WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', page, event_type) AS page_event_type_key,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE event_time >= '2025-01-28'
      AND event_time > DATE '2023-04-25'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
with_dims AS (
    -- Attach sessions attributes
    SELECT a.*, s.country
    FROM sessionized a
    LEFT JOIN sessions s
        ON a.session_id = s.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY page ORDER BY duration_sec DESC, id ASC) AS duration_sec_bucket,
        ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY island_id ASC, id ASC) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec ASC, id ASC) AS seq_no_2,
        SUM(duration_sec) OVER (PARTITION BY DATE_TRUNC('YEAR', event_time), country) AS event_time_period_sum_duration_sec,
        SUM(is_new_session) OVER (PARTITION BY page ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_new_session
    FROM with_dims
)
-- Window metrics over several different partitions
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY page ORDER BY island_id DESC, id ASC) AS row_seq,
    FIRST_VALUE(event_time_period_sum_duration_sec, TRUE) OVER w_hist AS first_known_event_time_period_sum_duration_sec,
    SUM(island_id) OVER (PARTITION BY country, session_id ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS island_id_sum_7d,
    LEAD(seq_no_2, 1, 0) OVER w_grp AS next_seq_no_2,
    AVG(seq_no_2) OVER (ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS seq_no_2_avg_90d
FROM scored
WINDOW w_hist AS (PARTITION BY country ORDER BY event_time_period_sum_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY session_id ORDER BY is_new_session ASC, id ASC);
