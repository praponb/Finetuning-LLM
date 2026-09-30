WITH sessions_by_customer AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(browser))) AS browser_values, SORT_ARRAY(COLLECT_SET(device)) AS device_list
    FROM sessions
    WHERE browser IN ('Edge', 'Firefox', 'Safari')
    GROUP BY customer_id
)
SELECT
    c.id,
    c.city,
    sbc.browser_values,
    sbc.device_list
FROM customers c
INNER JOIN sessions_by_customer sbc ON c.id = sbc.customer_id
ORDER BY city DESC, c.id DESC;
