WITH with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        ROW_NUMBER() OVER (ORDER BY started_at, id) AS event_seq
    FROM sessions
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY started_at, id)
),
scored AS (
    SELECT
        *,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY browser), 4) AS event_seq_share,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY country, device), 4) AS event_seq_share_2,
        ROW_NUMBER() OVER (ORDER BY event_seq, id) AS event_seq_2,
        SUM(row_seq) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY row_seq DESC, id) AS row_seq_bucket
    FROM with_windows
),
with_windows_sessions AS (
    SELECT
        *,
        LAG(event_seq_share_2, 1) OVER (PARTITION BY customer_id, country ORDER BY row_seq_bucket, id) AS prev_event_seq_share_2,
        PERCENT_RANK() OVER (ORDER BY event_seq_2) AS event_seq_2_percent_rank,
        MAX(row_seq) OVER (PARTITION BY device) AS device_max_row_seq,
        AVG(event_seq) OVER (PARTITION BY device, country ORDER BY event_seq_share, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS event_seq_avg_5,
        ROW_NUMBER() OVER (ORDER BY row_seq, id) AS seq_no
    FROM scored
),
cleaned AS (
    SELECT
        id,
        device,
        browser,
        started_at,
        row_seq,
        event_seq,
        event_seq_share,
        event_seq_share_2,
        cumulative_row_seq,
        row_seq_bucket,
        prev_event_seq_share_2,
        device_max_row_seq,
        event_seq_avg_5,
        seq_no,
        CAST(started_at AS DATE) AS started_at_day,
        CEIL(event_seq_share_2) AS event_seq_share_2_whole
    FROM with_windows_sessions
    WHERE started_at > DATE '2023-11-23'
      AND customer_id IS NOT NULL
      AND event_seq != 7
)
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    seq_no,
    COUNT(*) AS cnt,
    COUNT_IF(browser = 'Firefox') AS firefox_events,
    SIZE(COLLECT_SET(browser)) AS num_browsers
FROM cleaned
GROUP BY DATE_TRUNC('WEEK', started_at), seq_no
ORDER BY num_browsers DESC;
