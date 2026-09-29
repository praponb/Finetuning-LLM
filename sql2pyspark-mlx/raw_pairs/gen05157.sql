WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.coupon_codes,
        line.product_id,
        line.qty,
        line.price
    FROM carts t
    LATERAL VIEW EXPLODE(t.items) e AS line
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        price,
        RANK() OVER (ORDER BY price DESC) AS price_rank,
        RANK() OVER (ORDER BY price) AS price_rank_2,
        PERCENT_RANK() OVER (ORDER BY qty DESC NULLS FIRST) AS qty_percent_rank
    FROM exploded
    WHERE product_id >= 6
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        price_rank,
        price_rank_2,
        qty_percent_rank - price_rank AS qty_percent_rank_minus_price_rank
    FROM metrics
    WHERE qty_percent_rank > 0.43
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(price_rank_2, price_rank) AS greatest_price_rank_2_price_rank
    FROM valid
    WHERE qty_percent_rank_minus_price_rank < 31
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(price_rank_2, price_rank) AS greatest_price_rank_2_price_rank_2
    FROM filtered
    WHERE price_rank < 6
),
enriched AS (
    -- Ranking and gap features
    SELECT
        price_rank,
        price_rank_2,
        greatest_price_rank_2_price_rank,
        RANK() OVER (ORDER BY price_rank_2) AS price_rank_2_rank,
        CUME_DIST() OVER (ORDER BY greatest_price_rank_2_price_rank NULLS FIRST) AS greatest_price_rank_2_price_rank_cume_dist,
        PERCENT_RANK() OVER (ORDER BY greatest_price_rank_2_price_rank NULLS FIRST) AS greatest_price_rank_2_price_rank_percent_rank
    FROM base
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN greatest_price_rank_2_price_rank >= 88 THEN 'LARGE' ELSE 'SMALL' END AS greatest_price_rank_2_price_rank_tier,
        APPROX_PERCENTILE(price_rank_2_rank, ARRAY(0.25, 0.5, 0.75)) AS price_rank_2_rank_quartiles,
        SUM(price_rank_2) AS total_price_rank_2,
        MIN(greatest_price_rank_2_price_rank) AS min_greatest_price_rank_2_price_rank
    FROM enriched
    GROUP BY 1
)
SELECT
    greatest_price_rank_2_price_rank_tier,
    price_rank_2_rank_quartiles,
    min_greatest_price_rank_2_price_rank
FROM totals
ORDER BY greatest_price_rank_2_price_rank_tier;
