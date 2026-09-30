WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        base.id,
        base.coupon_codes,
        pos,
        line.qty,
        line.price,
        line.promo
    FROM carts base
    LATERAL VIEW POSEXPLODE(base.items) t AS pos, line
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        coupon_codes,
        pos,
        price,
        promo,
        NTILE(3) OVER (PARTITION BY promo ORDER BY qty, id, pos) qty_bucket,
        MAX(pos) OVER (PARTITION BY promo) promo_max_pos,
        AVG(pos) OVER w_grp pos_avg_6_prior,
        DENSE_RANK() OVER w_part qty_dense_rank
    FROM flattened
    WINDOW w_grp AS (ORDER BY pos, id, pos ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), w_part AS (PARTITION BY promo ORDER BY qty)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        coupon_codes,
        pos,
        price,
        promo,
        qty_bucket,
        promo_max_pos,
        pos_avg_6_prior,
        qty_dense_rank,
        pos_avg_6_prior / NULLIF(promo_max_pos, 0) pos_avg_6_prior_per_promo_max_pos,
        COALESCE(promo, 'UNKNOWN') promo_clean
    FROM enriched
    WHERE promo IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo ORDER BY pos_avg_6_prior DESC NULLS FIRST, id, pos) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo ORDER BY price DESC NULLS LAST, id, pos) = 1
)
SELECT
    id,
    coupon_codes,
    pos,
    price,
    qty_bucket,
    promo_max_pos,
    pos_avg_6_prior,
    qty_dense_rank,
    promo_clean
FROM latest
ORDER BY promo_max_pos
