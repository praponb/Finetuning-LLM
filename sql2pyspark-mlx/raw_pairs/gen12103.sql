WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        item.product_id,
        item.price,
        item.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            EXPLODE(items) AS item
        FROM carts
    ) t
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(product_id) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS product_id_max_15m,
        AVG(price) OVER (PARTITION BY YEAR(created_at), promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS price_avg_30m,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY product_id DESC NULLS LAST) AS product_id_percent_rank
    FROM lines
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, d.credit_limit
    FROM scored a
    LEFT JOIN customers d
        ON a.customer_id = d.id
    WHERE d.country = 'UK'
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT src.*, b.city
    FROM with_dims src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
with_dims_carts_2 AS (
    -- Attach customers attributes
    SELECT src.*, p.email
    FROM with_dims_carts src
    INNER JOIN customers p
        ON src.customer_id <=> p.id
)
-- Grouped metrics
SELECT
    customer_id,
    promo,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    ROUND(SUM(price) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS price_per_new_york,
    MAX(created_at) AS ended_at,
    COUNT(CASE WHEN promo = 'WELCOME5' THEN 1 END) AS welcome5_count
FROM with_dims_carts_2
GROUP BY CUBE (customer_id, promo)
HAVING ROUND(SUM(price) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) > 0
ORDER BY promo DESC
