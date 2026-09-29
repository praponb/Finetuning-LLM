WITH not_flagged AS (
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time <= s.started_at
        )
),
flagged AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM not_flagged
),
sessions_sessions AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER w_ordered AS session_no_dense_rank,
        LEAD(session_start_flag, 1) OVER (PARTITION BY YEAR(started_at), country ORDER BY session_start_flag, id) AS next_session_start_flag
    FROM sessions_sessions
    WHERE started_at < '2025-09-01'
    WINDOW w_ordered AS (PARTITION BY device ORDER BY session_no)
),
scored_sessions AS (
    SELECT
        id,
        customer_id,
        country,
        next_session_start_flag,
        LEAD(session_no) OVER w_ordered AS next_session_no,
        LAG(session_no) OVER w_ordered AS prev_session_no,
        LAG(session_start_flag) OVER w_ordered AS prev_session_start_flag,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM scored
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY next_session_start_flag, id)
),
latest_match AS (
    SELECT
        s.*,
        top1.duration_sec,
        top1.event_time
    FROM scored_sessions s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec, c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.next_session_start_flag,
    s.next_session_no,
    s.prev_session_no,
    s.prev_session_start_flag,
    s.event_seq,
    s.event_time,
    (SELECT MAX(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS max_order_discount
FROM latest_match s
