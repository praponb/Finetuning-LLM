WITH parsed AS (
    SELECT
        id,
        coupon_codes,
        items,
        ship_to,
        COALESCE(ship_to.country, 'UNKNOWN') AS country_or_unknown,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg
    FROM carts
)
SELECT
    id,
    coupon_codes,
    items,
    ship_to,
    country_or_unknown,
    weight_kg,
    NTILE(3) OVER w_running AS weight_kg_bucket,
    CUME_DIST() OVER (ORDER BY weight_kg DESC NULLS LAST) AS weight_kg_cume_dist
FROM parsed
WINDOW w_running AS (ORDER BY weight_kg DESC, id)
