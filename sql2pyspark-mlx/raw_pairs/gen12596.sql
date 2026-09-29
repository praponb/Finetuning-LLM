WITH latest_match AS (
    SELECT s.*, x.order_discount
    FROM stores s
    JOIN LATERAL (
            SELECT c.discount AS order_discount
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    CASE WHEN city RLIKE '^(Tokyo|Chiang Mai)$' THEN 'CORE' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS city_zone
FROM latest_match
ORDER BY city DESC
