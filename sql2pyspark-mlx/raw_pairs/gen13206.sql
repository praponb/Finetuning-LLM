WITH enriched AS (
    SELECT
        id,
        device,
        started_at,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        FIRST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY TO_DATE(started_at), country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser
    FROM sessions
),
eligible AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.started_at
        )
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS seq_no_2,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS event_seq
    FROM eligible
),
with_latest AS (
    SELECT
        s.*,
        latest.duration_sec,
        latest.web_event_page
    FROM metrics s
    JOIN LATERAL (
            SELECT c.duration_sec, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    device,
    started_at,
    seq_no,
    seq_no_2,
    event_seq,
    duration_sec,
    AVG(event_seq) OVER w_recent AS event_seq_avg_3_prior,
    ROW_NUMBER() OVER (PARTITION BY device ORDER BY seq_no, id) AS seq_no_3,
    ROUND(seq_no_2 / SUM(seq_no_2) OVER (PARTITION BY device), 4) AS seq_no_2_share,
    PERCENT_RANK() OVER (PARTITION BY MONTH(started_at), device ORDER BY duration_sec NULLS FIRST) AS duration_sec_percent_rank,
    ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY web_event_page), 4) AS seq_no_share
FROM with_latest
WINDOW w_recent AS (PARTITION BY TO_DATE(started_at) ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING);
