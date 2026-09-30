WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        base.id,
        base.customer_id,
        base.coupon_codes,
        pos + 1 AS position,
        item.qty,
        item.price
    FROM carts base
    LATERAL VIEW OUTER POSEXPLODE(base.items) e AS pos, item
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        qty,
        price,
        tag
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) e AS tag
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM lines_carts
    INTERSECT ALL
    SELECT id, customer_id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines_carts s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(price) OVER w_part AS tag_count_price,
        ROUND(qty / SUM(qty) OVER (PARTITION BY tag, customer_id), 4) AS qty_share,
        DENSE_RANK() OVER (ORDER BY price DESC) AS price_dense_rank,
        PERCENT_RANK() OVER w_hist AS qty_percent_rank,
        ROUND(qty / SUM(qty) OVER (PARTITION BY tag), 4) AS qty_share_2
    FROM verified
    WINDOW w_part AS (PARTITION BY tag, customer_id), w_hist AS (ORDER BY qty NULLS LAST)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN qty_share_2 >= 0.87 THEN 'PREMIUM' ELSE 'STANDARD' END AS qty_share_2_tier,
        PERCENTILE_APPROX(qty_share, 0.9) AS qty_share_p90,
        ROUND(SUM(qty) / NULLIF(COUNT(CASE WHEN tag = 'VIP' THEN 1 END), 0), 2) AS qty_per_vip
    FROM enriched
    GROUP BY qty_share_2_tier
)
SELECT qty_share_2_tier, qty_per_vip
FROM stats
ORDER BY qty_share_2_tier DESC;
