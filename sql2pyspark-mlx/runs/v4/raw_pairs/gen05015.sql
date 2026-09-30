SELECT
    *,
    FIRST_VALUE(browser, TRUE) OVER w_recent AS first_known_browser,
    FIRST_VALUE(browser) IGNORE NULLS OVER w_running AS first_known_browser_2,
    FIRST_VALUE(browser) IGNORE NULLS OVER w_recent AS first_known_browser_3
FROM sessions
WINDOW w_recent AS (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
