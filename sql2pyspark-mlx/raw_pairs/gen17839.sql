WITH with_gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM payments
),
sessions_payments AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    status,
    order_id,
    CASE WHEN is_new_session >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_new_session_tier,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(*) AS num_rows,
    COUNT(*) AS num_rows_2,
    GROUPING(status) AS grouping_id
FROM sessions_payments
GROUP BY ROLLUP (status, order_id, CASE WHEN is_new_session >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING COUNT(*) > 1;
