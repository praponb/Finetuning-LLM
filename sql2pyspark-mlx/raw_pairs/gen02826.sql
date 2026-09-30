WITH latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        latest.store_sale_sale_date
    FROM stores s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', store_sale_sale_date) AS quarter_start,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT_IF(city = 'Berlin') AS berlin_events
FROM latest_match
GROUP BY DATE_TRUNC('QUARTER', store_sale_sale_date)
HAVING num_cities > 1
ORDER BY num_cities DESC
