WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.product_id,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) x AS line
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        product_id,
        EXPLODE(coupon_codes) AS code
    FROM exploded
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        created_at,
        code,
        COALESCE(product_id, 0) AS product_id_filled,
        HOUR(created_at) AS created_at_hour
    FROM exploded_carts
    WHERE code IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM valid
    EXCEPT
    SELECT id, customer_id
    FROM carts
    WHERE SIZE(coupon_codes) >= 2
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        MIN(created_at_hour) OVER (PARTITION BY code) AS code_min_created_at_hour,
        MAX(created_at_hour) OVER (PARTITION BY code ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS created_at_hour_max_1h,
        PERCENT_RANK() OVER (PARTITION BY code ORDER BY product_id_filled DESC) AS product_id_filled_percent_rank,
        COUNT(created_at_hour) OVER (PARTITION BY MONTH(created_at) ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS created_at_hour_count_2h,
        AVG(created_at_hour) OVER (PARTITION BY YEAR(created_at), customer_id) AS created_at_year_avg_created_at_hour
    FROM reconciled
    WHERE product_id_filled < 28
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.created_at_hour_max_1h,
        e.product_id_filled_percent_rank,
        e.created_at_hour_count_2h,
        b.country
    FROM with_windows e
    INNER JOIN customers b
        ON e.customer_id = b.id
),
joined_carts AS (
    -- Attach customers attributes
    SELECT e.*, c.city
    FROM joined e
    LEFT JOIN customers c
        ON e.customer_id = c.id
)
-- Join a dimension and aggregate
SELECT
    c.segment,
    c.city AS customer_city,
    s.country AS cart_country,
    COUNT(*) AS rows_total,
    MIN(s.created_at_hour_count_2h) AS min_created_at_hour_count_2h,
    GROUPING_ID() AS level_id
FROM joined_carts s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY GROUPING SETS ((c.segment, c.city, s.country), (c.segment, c.city), (c.segment))
ORDER BY customer_city
