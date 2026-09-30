WITH flagged AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessions_sessions AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.started_at,
    s.session_start_flag,
    (SELECT MAX(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS max_account_balance
FROM sessions_sessions s
ORDER BY started_at DESC;
