WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(product_id) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) product_id_avg_1h,
        RANK() OVER (PARTITION BY YEAR(created_at), customer_id ORDER BY qty NULLS LAST) qty_rank,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY price NULLS LAST) price_percent_rank,
        MIN(qty) OVER (PARTITION BY customer_id) customer_id_min_qty
    FROM flattened
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.coupon_codes,
        e.product_id,
        e.qty,
        e.price,
        e.promo,
        e.price_percent_rank,
        e.customer_id_min_qty,
        c.city
    FROM with_windows e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
    WHERE c.country IN ('TH', 'US', 'DE')
),
enriched_carts AS (
    -- Ranking and gap features
    SELECT *, ROUND(price / SUM(price) OVER (PARTITION BY promo, customer_id), 4) price_share
    FROM enriched
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN price >= 229 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
        CASE WHEN customer_id_min_qty >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END AS customer_id_min_qty_tier,
        promo,
        SUM(CASE WHEN promo = 'WELCOME5' THEN qty ELSE 0 END) AS welcome5_qty,
        ROUND(AVG(price_percent_rank), 2) AS avg_price_percent_rank
    FROM enriched_carts
    GROUP BY 1, 2, 3
    HAVING ROUND(AVG(price_percent_rank), 2) > 0
)
SELECT
    price_tier,
    customer_id_min_qty_tier,
    promo,
    welcome5_qty
FROM totals
ORDER BY welcome5_qty DESC;
