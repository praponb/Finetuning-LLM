WITH enriched AS (
    SELECT
        id,
        browser,
        started_at,
        country,
        ROW_NUMBER() OVER (PARTITION BY browser, device ORDER BY started_at, id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) seq_no_2,
        ROW_NUMBER() OVER w_recent row_seq,
        LAST_VALUE(browser) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_browser
    FROM sessions
    WHERE customer_id IN (2, 19, 37)
    WINDOW w_recent AS (ORDER BY started_at DESC, id)
)
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    AVG(row_seq) FILTER (WHERE browser = 'Firefox') AS firefox_avg_row_seq,
    ROUND(AVG(seq_no_2), 2) AS avg_seq_no_2,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes
FROM enriched
GROUP BY DATE_TRUNC('YEAR', started_at)
ORDER BY avg_seq_no_2 DESC NULLS LAST;
