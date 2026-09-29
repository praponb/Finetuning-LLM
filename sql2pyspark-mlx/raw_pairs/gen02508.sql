WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.launch_date,
        top1.product_price
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.launch_date, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    country,
    city,
    rating,
    launch_date,
    CASE WHEN city RLIKE '^(Bangkok|Berlin)$' THEN 'OTHER_ZONE' WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'GROUP_B' ELSE 'CORE' END AS city_zone
FROM with_last
