WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        src.coupon_codes,
        rec.product_id,
        rec.qty,
        rec.promo
    FROM carts src
    LATERAL VIEW OUTER EXPLODE(src.items) e AS rec
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.created_at,
        a.product_id,
        a.qty,
        a.promo,
        p.country,
        p.email,
        p.name
    FROM flattened a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM with_dims
    EXCEPT
    SELECT id, customer_id
    FROM carts
    WHERE TO_DATE(created_at) >= '2024-04-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        qty,
        promo,
        email,
        name,
        ROUND(qty / SUM(qty) OVER (PARTITION BY customer_id, country), 4) AS qty_share,
        SUM(qty) OVER w_ordered AS qty_sum_15m,
        ROUND(qty / SUM(qty) OVER (PARTITION BY country), 4) AS qty_share_2
    FROM kept_rows
    WINDOW w_ordered AS (PARTITION BY country ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
metrics_carts AS (
    -- Ranking and gap features
    SELECT
        name,
        qty_share,
        qty_sum_15m,
        qty_share_2,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY qty_share_2 DESC NULLS FIRST) AS qty_share_2_cume_dist
    FROM metrics
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        qty_share,
        ROUND(qty_share_2 * 1.07, 2) AS qty_share_2_adjusted,
        qty_share + qty_sum_15m AS qty_share_plus_qty_sum_15m
    FROM metrics_carts
    WHERE name IS NULL
)
SELECT
    qty_share,
    qty_share_2_adjusted,
    qty_share_plus_qty_sum_15m
FROM cleaned
ORDER BY qty_share_2_adjusted DESC NULLS FIRST
