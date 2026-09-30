WITH flattened_attrs AS (
    SELECT
        id,
        coupon_codes,
        attributes,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
        FORALL(items, item -> item.qty <= 5) AS all_small_qty,
        ARRAY_MAX(TRANSFORM(items, item -> item.price)) AS max_item_price,
        attributes['utm_source'] AS utm_source
    FROM carts
)
SELECT
    CASE WHEN max_item_price >= 208 THEN 'LARGE' ELSE 'SMALL' END AS max_item_price_tier,
    CASE WHEN weight_kg >= 3.0 THEN 'HIGH' ELSE 'LOW' END AS weight_kg_tier,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM flattened_attrs
GROUP BY CASE WHEN max_item_price >= 208 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN weight_kg >= 3.0 THEN 'HIGH' ELSE 'LOW' END
ORDER BY weight_kg_tier DESC NULLS FIRST;
