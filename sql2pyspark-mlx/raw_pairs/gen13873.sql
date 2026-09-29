WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        item.product_id,
        item.price
    FROM (
        SELECT
            id,
            coupon_codes,
            EXPLODE_OUTER(items) AS item
        FROM carts
    ) x
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY product_id DESC NULLS LAST) AS product_id_rank,
        PERCENT_RANK() OVER (ORDER BY price DESC NULLS FIRST) AS price_percent_rank,
        RANK() OVER (ORDER BY price DESC NULLS FIRST) AS price_rank,
        DENSE_RANK() OVER (ORDER BY product_id DESC NULLS LAST) AS product_id_dense_rank
    FROM lines
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY product_id_dense_rank DESC NULLS FIRST) AS product_id_dense_rank_rank,
        PERCENT_RANK() OVER (ORDER BY price_rank NULLS LAST) AS price_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY product_id_dense_rank DESC NULLS LAST) AS product_id_dense_rank_percent_rank
    FROM metrics
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(product_id_rank * 100, 2) AS product_id_rank_scaled
    FROM enriched
    WHERE price < 170
),
grouped AS (
    -- Grouped metrics
    SELECT
        product_id,
        MIN(price) AS min_price,
        MAX(product_id) AS max_product_id,
        ROUND(AVG(product_id_rank_scaled), 2) AS avg_product_id_rank_scaled
    FROM valid
    GROUP BY product_id
)
SELECT
    product_id,
    min_price,
    max_product_id
FROM grouped;
