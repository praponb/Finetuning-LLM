WITH flattened_attrs AS (
    SELECT
        id,
        coupon_codes,
        TRANSFORM(items, item -> item.product_id) AS product_ids,
        ship_to.country AS ship_country,
        AGGREGATE(items, 0.0D, (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS items_total,
        EXISTS(items, item -> item.price > 100) AS has_premium_item
    FROM carts
)
SELECT
    ship_country,
    CASE WHEN items_total >= 1250 THEN 'PREMIUM' ELSE 'STANDARD' END AS items_total_tier,
    APPROX_PERCENTILE(items_total, 0.5) AS items_total_p50,
    AVG(items_total) FILTER (WHERE ship_country = 'US') AS us_avg_items_total
FROM flattened_attrs
GROUP BY 1, 2
HAVING APPROX_PERCENTILE(items_total, 0.5) > 0
