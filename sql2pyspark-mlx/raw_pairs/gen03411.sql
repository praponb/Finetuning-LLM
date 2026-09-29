WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        pos,
        line.product_id,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) x AS pos, line
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share,
        COUNT(price) OVER (PARTITION BY customer_id) AS customer_id_count_price,
        MAX(pos) OVER w_recent AS customer_id_max_pos
    FROM flattened
    WINDOW w_recent AS (PARTITION BY customer_id, promo)
),
metrics_carts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY customer_id_max_pos NULLS LAST) AS customer_id_max_pos_dense_rank,
        ROUND(price / SUM(price) OVER (PARTITION BY customer_id, promo), 4) AS price_share
    FROM metrics
),
metrics_carts_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        price,
        promo,
        customer_id_count_price,
        customer_id_max_pos,
        customer_id_max_pos_dense_rank,
        price_share,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY customer_id_max_pos DESC NULLS FIRST) AS customer_id_max_pos_cume_dist,
        CUME_DIST() OVER (ORDER BY pos DESC NULLS FIRST) AS pos_cume_dist,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY customer_id_max_pos) AS customer_id_max_pos_cume_dist_2,
        AVG(customer_id_max_pos) OVER w_grp AS customer_id_avg_customer_id_max_pos,
        STDDEV_SAMP(product_id_share) OVER w_part AS customer_id_stddev_samp_product_id_share
    FROM metrics_carts
    WHERE price < 80
    WINDOW w_grp AS (PARTITION BY customer_id, promo), w_part AS (PARTITION BY customer_id)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(customer_id_max_pos_cume_dist_2 AS INT) AS customer_id_max_pos_cume_dist_2_int
    FROM metrics_carts_2
    WHERE promo IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.promo,
        s.customer_id_count_price,
        s.customer_id_max_pos_dense_rank,
        s.customer_id_max_pos_cume_dist_2_int,
        c.email,
        c.city
    FROM valid s
    LEFT JOIN customers c
        ON s.customer_id <=> c.id
)
-- Ranking and gap features
SELECT
    customer_id,
    promo,
    customer_id_count_price,
    customer_id_max_pos_dense_rank,
    city,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY customer_id_max_pos_cume_dist_2_int DESC NULLS LAST) AS customer_id_max_pos_cume_dist_2_int_dense_rank,
    ROUND(customer_id_count_price / SUM(customer_id_count_price) OVER (PARTITION BY city), 4) AS customer_id_count_price_share
FROM enriched;
