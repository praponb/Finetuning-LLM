WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM (
        SELECT
            id,
            customer_id,
            EXPLODE_OUTER(items) AS item
        FROM carts
    ) li
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.price,
        p.signup_date
    FROM exploded s
    INNER JOIN customers p
        ON s.customer_id <=> p.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM enriched
    EXCEPT
    SELECT customer_id
    FROM carts
    WHERE created_at >= '2025-03-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        signup_date,
        MIN(price) OVER (PARTITION BY DATE_TRUNC('month', signup_date), customer_id) AS signup_date_period_min_price,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY price) AS price_dense_rank,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY price DESC) AS price_dense_rank_2
    FROM reconciled
)
SELECT
    customer_id,
    signup_date,
    price_dense_rank_2
FROM metrics;
