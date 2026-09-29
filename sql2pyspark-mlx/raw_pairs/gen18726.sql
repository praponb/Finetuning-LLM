WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.created_at,
        s.price,
        s.promo,
        p.name
    FROM lines s
    LEFT JOIN customers p
        ON s.customer_id = p.id
    WHERE p.is_active
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        created_at,
        price,
        promo,
        name,
        CASE WHEN price >= 217 THEN 'High' WHEN price >= 55 THEN 'Medium' ELSE 'Low' END AS price_tier,
        COALESCE(promo, 'DIRECT') AS promo_clean
    FROM joined
    WHERE promo IS NOT DISTINCT FROM 'VIP'
      AND created_at >= DATE '2025-03-02'
),
enriched AS (
    -- Attach customers attributes
    SELECT s.*, p.city
    FROM filtered s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
enriched_carts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(price) OVER (PARTITION BY TO_DATE(created_at) ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS price_max_15m,
        AVG(price) OVER (PARTITION BY DATE_TRUNC('WEEK', created_at), promo) AS created_at_period_avg_price,
        ROUND(price / SUM(price) OVER (PARTITION BY YEAR(created_at), customer_id), 4) AS price_share
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        city,
        price_share,
        price
    FROM enriched_carts
) src
UNPIVOT (
    value FOR metric IN (price_share, price)
);
