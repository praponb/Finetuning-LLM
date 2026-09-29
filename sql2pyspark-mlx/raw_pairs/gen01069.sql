WITH exploded AS (
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS code
    FROM carts
)
SELECT
    customer_id,
    code,
    SORT_ARRAY(COLLECT_SET(code)) AS code_list,
    COUNT(*) AS event_count,
    SORT_ARRAY(COLLECT_SET(code)) AS code_list_2,
    SORT_ARRAY(COLLECT_SET(code)) AS code_list_3,
    COUNT_IF(code = 'SPRING10') AS spring10_events,
    COUNT(CASE WHEN code = 'WELCOME5' THEN 1 END) AS welcome5_count
FROM exploded
GROUP BY 1, 2
ORDER BY spring10_events DESC
