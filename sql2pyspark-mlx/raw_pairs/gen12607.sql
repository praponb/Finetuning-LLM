WITH metrics AS (
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        NTILE(4) OVER (ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_avg_2
    FROM web_events
),
with_dims AS (
    SELECT
        a.id,
        a.session_id,
        a.event_time,
        a.duration_sec,
        b.country
    FROM metrics a
    LEFT JOIN sessions b
        ON a.session_id = b.id
),
joined AS (
    SELECT
        src.*,
        s.device,
        s.browser
    FROM with_dims src
    LEFT JOIN sessions s
        ON src.session_id = s.id
    WHERE s.country NOT IN ('SG', 'JP')
),
metrics_web_events AS (
    SELECT
        id,
        event_time,
        browser,
        LEAD(duration_sec) OVER w_hist AS next_duration_sec,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM joined
    WINDOW w_hist AS (PARTITION BY country ORDER BY duration_sec, id)
)
SELECT
    CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    SUM(CASE WHEN browser = 'Safari' THEN row_seq ELSE 0 END) AS safari_row_seq,
    SUM(CASE WHEN browser = 'Firefox' THEN next_duration_sec ELSE 0 END) AS firefox_next_duration_sec,
    SUM(row_seq) AS total_row_seq,
    SUM(next_duration_sec) FILTER (WHERE browser = 'Edge') AS edge_total_next_duration_sec,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list
FROM metrics_web_events
GROUP BY row_seq_tier
HAVING firefox_next_duration_sec < 1000000
ORDER BY total_row_seq NULLS LAST;
