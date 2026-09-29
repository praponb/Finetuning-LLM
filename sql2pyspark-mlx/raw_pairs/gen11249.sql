WITH unpacked AS (
    SELECT
        id,
        items,
        FORALL(items, x -> x.qty <= 5) AS all_small_qty,
        SIZE(attributes) AS num_attributes,
        AGGREGATE(items, 0D, (acc, x) -> acc + x.qty * x.price, acc -> ROUND(acc, 2)) AS basket_amount
    FROM carts
)
SELECT
    CASE WHEN basket_amount >= 1780 THEN 'HIGH' ELSE 'LOW' END AS basket_amount_tier,
    CASE WHEN num_attributes >= 68 THEN 'LARGE' ELSE 'SMALL' END AS num_attributes_tier,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(basket_amount, 0.5) AS basket_amount_p50,
    AVG(num_attributes) AS avg_num_attributes,
    COUNT(DISTINCT id) AS distinct_ids
FROM unpacked
GROUP BY CASE WHEN basket_amount >= 1780 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN num_attributes >= 68 THEN 'LARGE' ELSE 'SMALL' END
HAVING basket_amount_p50 > 0
ORDER BY num_attributes_tier NULLS LAST;
