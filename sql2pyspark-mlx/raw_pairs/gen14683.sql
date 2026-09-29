WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        product_id,
        promo,
        code
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) x AS code
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        product_id,
        STDDEV_SAMP(product_id) OVER (PARTITION BY code) code_stddev_samp_product_id,
        RANK() OVER (ORDER BY product_id DESC) product_id_rank,
        CUME_DIST() OVER (PARTITION BY code ORDER BY product_id DESC) product_id_cume_dist
    FROM lines
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.code_stddev_samp_product_id,
        s.product_id_rank,
        c.age
    FROM with_windows s
    LEFT JOIN customers c
        ON s.customer_id <=> c.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, code_stddev_samp_product_id * age code_stddev_samp_product_id_times_age
    FROM joined
    WHERE product_id_rank <= 4
)
-- Join a dimension and aggregate
SELECT
    d.country,
    d.city,
    COUNT(*) AS cnt,
    AVG(src.product_id_rank) AS avg_product_id_rank
FROM valid src
LEFT JOIN customers d ON src.customer_id = d.id
GROUP BY GROUPING SETS ((d.country, d.city), (d.country))
ORDER BY cnt NULLS LAST
