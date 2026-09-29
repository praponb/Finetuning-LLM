SELECT
    YEAR(event_time) AS event_year,
    MONTH(event_time) AS event_month,
    page,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
    SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END) AS checkout_count
FROM web_events
GROUP BY YEAR(event_time), MONTH(event_time), page
ORDER BY 3
