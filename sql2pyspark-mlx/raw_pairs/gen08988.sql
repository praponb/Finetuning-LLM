WITH unpacked AS (
    SELECT
        id,
        ship_to,
        ship_to.city AS ship_city,
        ARRAY_CONTAINS(coupon_codes, 'VIP') AS used_vip,
        AGGREGATE(items, 0.0D, (acc, i) -> acc + i.qty * i.price, acc -> ROUND(acc, 2)) AS items_total,
        FORALL(items, i -> i.qty <= 5) AS all_small_qty,
        COALESCE(ship_to.city, 'UNKNOWN') AS city_or_unknown
    FROM carts
)
SELECT
    CASE WHEN items_total >= 1220 THEN 'HIGH' ELSE 'LOW' END AS items_total_tier,
    COUNT(*) AS cnt,
    ROUND(SUM(items_total) / NULLIF(COUNT(CASE WHEN ship_city = 'Tokyo' THEN 1 END), 0), 2) AS items_total_per_tokyo,
    COUNT_IF(ship_city = 'Chiang Mai') AS chiang_mai_events
FROM unpacked
GROUP BY CASE WHEN items_total >= 1220 THEN 'HIGH' ELSE 'LOW' END
