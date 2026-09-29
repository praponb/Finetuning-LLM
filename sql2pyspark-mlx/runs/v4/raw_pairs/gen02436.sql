WITH scored AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        LAST_VALUE(country, TRUE) OVER w_running AS last_known_country,
        LAST_VALUE(country, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country_2,
        ROW_NUMBER() OVER (ORDER BY started_at, id) AS seq_no,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY browser, device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        FIRST_VALUE(browser, TRUE) OVER w_recent AS first_known_browser
    FROM sessions
    WINDOW w_running AS (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN seq_no >= 7 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
    COUNT(CASE WHEN browser = 'Edge' THEN 1 END) AS edge_count,
    MAX(seq_no) FILTER (WHERE device = 'mobile') AS mobile_max_seq_no,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    MIN(started_at) AS first_started_at,
    MAX(started_at) AS last_seen
FROM scored
GROUP BY CASE WHEN seq_no >= 7 THEN 'LARGE' ELSE 'SMALL' END
HAVING edge_count < 1
ORDER BY avg_seq_no DESC NULLS FIRST;
