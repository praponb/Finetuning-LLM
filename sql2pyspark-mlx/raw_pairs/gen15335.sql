SELECT
    id,
    started_at,
    browser,
    customer_id,
    DENSE_RANK() OVER (PARTITION BY MONTH(started_at), browser ORDER BY started_at DESC) AS dense_rnk,
    PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY started_at DESC) AS started_at_pct_rank
FROM sessions
WHERE device IN ('mobile', 'tablet')
ORDER BY browser, dense_rnk DESC
